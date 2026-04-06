import json
import logging
import re
from time import sleep
import uuid

from ckan.types import Context
from requests import HTTPError
from sickle import Sickle
import sqlalchemy as sa
import html
import html_to_markdown

from ckan import model
import ckan.lib.plugins as lib_plugins
from ckan.plugins import toolkit

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.harvesters import HarvesterBase
from ckan.lib.navl.validators import unicode_safe


log = logging.getLogger(__name__)

invalid_name_characters_regexp = re.compile(r"\.|\:")


# Code is partially based on
#   https://github.com/kata-csc/ckanext-oaipmh/blob/master/ckanext/oaipmh/harvester.py
# and
#   https://github.com/ckan/ckanext-dcat/blob/master/ckanext/dcat/harvesters/rdf.py
#
# See also the protocol specification:
#   https://www.openarchives.org/OAI/openarchivesprotocol.html
class OAIPMHHarvesterPlugin(HarvesterBase):
    def info(self) -> dict[str, str]:
        """Returns a dictionary containing different descriptors of the harvester.
        The returned dictionary will contain:

        * name: machine-readable name of this harvester, as stored in the DB.
        * title: human-readable name. This will appear in the form's select box
          in the web UI.
        * description: a small description of what the harvester does. This
          will appear on the form as a guidance to the user.
        """

        return {
            "name": "oai-pmh",
            "title": "OAI-PMH",
            "description": "Harvests datasets from an endpoint compatible with the Open Archives Initiativ (OAI) Protocol for Metadata Harvesting (PMH)",
        }

    def validate_config(self, source_config):
        """Validates the configuration string entered into the form.
        Returns a single string (the validated and normalized config).

        If any part of the configuration is invalid, raises exceptions,
        which will be caught and shown to the admin user as the form's error messages.

        :param source_config: Config string coming from the form
        :returns: A string with the validated configuration options
        """
        if not source_config:
            return source_config

        try:
            source_config_obj = json.loads(source_config)
        except json.JSONDecodeError as error:
            raise ValueError(f"configuration must be valid JSON: {error}")

        if not isinstance(source_config_obj, dict):
            raise ValueError(
                f"configuration must be a JSON object: '{source_config_obj}'"
            )

        if "set" in source_config_obj:
            if not isinstance(source_config_obj["set"], str):
                raise ValueError("`set` must be a string")

        if "limit" in source_config_obj:
            if not isinstance(source_config_obj["limit"], int):
                raise ValueError("`limit` must be an integer")

        return source_config

    def get_original_url(self, harvest_object_id) -> str | None:
        """Returns the URL to the original remote document (back on the platform).

        Examples:
        * For a Zenodo record: https://zenodo.org/records/{id}

        :param harvest_object_id: HarvestObject id
        :returns: A string with the URL to the original document
        """
        obj = (
            model.Session.query(HarvestObject)
            .filter(HarvestObject.id == harvest_object_id)
            .first()
        )
        if obj is not None:
            return obj.source.url

        return None

    def _get_configuration(self, harvest_job):
        """Retrieves the configuration stored in the database for a given harvest object."""
        source_config = harvest_job.source.config
        if not source_config:
            self._save_gather_error(
                "Harvest job has no associated configuration",
                harvest_job,
            )
            return {}

        log.debug("Config: '%s'", source_config)

        try:
            configuration = json.loads(harvest_job.source.config)
        except ValueError as error:
            self._save_gather_error(
                f"Unable to parse configuration source as JSON: '{source_config}', {error}",
                harvest_job,
            )
            return {}

        return configuration

    def gather_stage(self, harvest_job):
        """Receives a harvest job object and constructs the list of objects
        (object identifiers) to be fetched later.

        This will also create the associated HarvestObjects in the database,
        specifing their GUID and linking them to this job.

        :param harvest_job: HarvestJob object
        :returns: A list of HarvestObject ids
        :type harvest_job: HarvestJob
        """

        source_url = harvest_job.source.url
        log.debug("Harvest source: %s", source_url)

        sickle = Sickle(source_url)

        config = self._get_configuration(harvest_job)

        filter_set = config.get("set", None)
        if filter_set is not None:
            log.debug("Configured set for filtering records: '%s'", filter_set)

        limit = config.get("limit", None)
        if limit is not None:
            log.debug("Configured limit for number of fetched records: %d", limit)

        if limit is not None:
            # Check if we already have more fetched/linked datasets in the DB
            # than the limit configured by the user.

            associated_packages = (
                model.Session.query(HarvestObject.package_id, HarvestObject.guid)
                .filter(HarvestObject.current)
                .distinct(HarvestObject.package_id)
                .all()
            )

            # Refetch the previously harvested datasets
            object_ids: list = []
            counter = 0
            for _, guid in associated_packages:
                obj = HarvestObject(guid=guid, job=harvest_job)
                obj.save()
                object_ids.append(obj.id)
                counter += 1

            if counter > 0:
                log.info(
                    "We already have %d harvested datasets from this source in the database, will add up to %d more",
                    len(object_ids),
                    limit - len(object_ids),
                )

        else:
            object_ids = []
            counter = 0

        if limit is None or counter < limit:
            # Query the list of identifiers of records from the given set
            headers = sickle.ListIdentifiers(metadataPrefix="oai_dc", set=filter_set)

            for header in headers:
                obj = HarvestObject(guid=header.identifier, job=harvest_job)
                obj.save()
                object_ids.append(obj.id)

                counter += 1
                if limit is not None and counter == limit:
                    break

        num_objects = len(object_ids)
        if num_objects == 0:
            self._save_gather_error(
                f"Gather: No records found for endpoint URL '{harvest_job.source.url}' and filter set '{filter_set}'",
                harvest_job,
            )
        else:
            log.info(
                "Gather stage found %d objects to harvest",
                num_objects,
            )

        return object_ids

    def fetch_stage(self, harvest_object):
        """The fetch stage receives a HarvestObject object and is responsible for:
        - getting the contents of the remote object (through the OAI-PMH API).
        - saving the content in the provided HarvestObject.
        - creating and storing any suitable HarvestObjectErrors that may occur.
        - returning True if everything went as expected, False otherwise.

        :param harvest_object: HarvestObject object
        :returns: True if everything went right, False if errors were found
        """
        log.debug("Fetching record with ID %s", harvest_object.guid)

        source_url = harvest_object.job.source.url
        sickle = Sickle(source_url)

        try:
            try:
                record = sickle.GetRecord(
                    identifier=harvest_object.guid, metadataPrefix="oai_dc"
                )
            except HTTPError as httperr:
                # Client error: too many requests
                if httperr.response.status_code == 429:
                    log.warning(
                        "Received error 429 too many requests from OAI-PMH API, waiting a bit then trying request again"
                    )

                    sleep_duration = 5
                    sleep(sleep_duration)

                    record = sickle.GetRecord(
                        identifier=harvest_object.guid, metadataPrefix="oai_dc"
                    )

                else:
                    raise

            metadata = record.get_metadata()

        except Exception as ex:
            self._save_object_error(
                f"Unable to get record with metadata from provider: {ex}",
                harvest_object,
            )
            return False

        if not metadata:
            self._save_object_error(
                f"Record '{record.identifier}' has no associated metadata",
                harvest_object,
            )
            return False

        harvest_object.content = json.dumps(metadata)
        harvest_object.save()

        return True

    # These were taken from the DCAT extension's harvester base class:
    # https://github.com/ckan/ckanext-dcat/blob/master/ckanext/dcat/harvesters/base.py
    def _read_datasets_from_db(self, guid):
        """
        Returns a database result of datasets matching the given GUID.
        """
        datasets = (
            model.Session.query(model.Package.id)
            .join(model.PackageExtra)
            .filter(model.PackageExtra.key == "guid")
            .filter(model.PackageExtra.value == guid)
            .filter(model.Package.state == "active")
            .all()
        )

        return datasets

    def _get_existing_dataset(self, guid):
        """
        Checks if a dataset with a certain guid extra already exists

        Returns a dict as the ones returned by package_show
        """

        datasets = self._read_datasets_from_db(guid)

        if not datasets:
            return None
        elif len(datasets) > 1:
            log.error(f"Found more than one dataset with the same GUID: {guid}")

        package_show = toolkit.get_action("package_show")
        return package_show({}, {"id": datasets[0][0]})

    def import_stage(self, harvest_object):
        """The import stage receives a HarvestObject object and is responsible for:
        - performing any necessary action with the fetched object (e.g.
          create, update or delete a CKAN package).
          Note: if this stage creates or updates a package, a reference
          to the package should be added to the HarvestObject.
        - setting the HarvestObject.package (if there is one)
        - setting the HarvestObject.current for this harvest:
        - True if successfully created/updated
        - False if successfully deleted
        - setting HarvestObject.current to False for previous harvest
        objects of this harvest source if the action was successful.
        - creating and storing any suitable HarvestObjectErrors that may
          occur.
        - creating the HarvestObject - Package relation (if necessary)
        - returning True if the action was done, "unchanged" if the object
          didn't need harvesting after all or False if there were errors.

        N.B.: You can run this stage repeatedly using 'paster harvest import'.

        :param harvest_object: HarvestObject object
        :returns: True if the action was done, "unchanged" if the object didn't
                need harvesting after all or False if there were errors.
        """

        log.debug("In OAIPMH `import_stage`")

        if harvest_object.content is None:
            self._save_object_error(
                f"Empty `content` field for object {harvest_object.id}",
                harvest_object,
                "Import",
            )
            return False

        try:
            metadata = json.loads(harvest_object.content)
        except ValueError:
            self._save_object_error(
                f"Could not parse content for object {harvest_object.id}",
                harvest_object,
                "Import",
            )
            return False

        # Only harvest dataset records
        if (
            len(metadata["type"]) != 1
            and metadata["type"][0] != "info:eu-repo/semantics/other"
        ):
            return "unchanged"

        # Get the last harvested object (if any)
        previous_object = (
            model.Session.query(HarvestObject)
            .filter(HarvestObject.guid == harvest_object.guid)
            .filter(HarvestObject.current)
            .first()
        )

        # Flag previous object as not current anymore
        if previous_object:
            previous_object.current = False
            previous_object.add()

        # Flag this object as the current one
        harvest_object.current = True
        harvest_object.add()

        context: Context = {
            "user": self._get_user_name(),
            "return_id_only": True,
            "ignore_auth": True,
        }

        package_plugin = lib_plugins.lookup_package_plugin(None)

        # Check if a dataset with the same guid exists
        existing_dataset = self._get_existing_dataset(harvest_object.guid)

        if "description" in metadata:
            description = "".join(metadata["description"])
            result = html_to_markdown.convert(html.unescape(description))
            description = result["content"]
        else:
            description = ""

        # Try to find an identifier which is an actual URL, or maybe a DOI
        def identifier_weight(id: str):
            if id.startswith(("http://", "https://")):
                return 2
            if "doi" in id:
                return 1
            return 0

        dataset_identifiers = sorted(metadata["identifier"], key=identifier_weight)
        name = invalid_name_characters_regexp.sub("_", dataset_identifiers[0])

        url = None
        for identifier in dataset_identifiers:
            if identifier.startswith(("http://", "https://")):
                url = identifier.strip()
                break

        dataset = {
            "title": "".join(metadata["title"]),
            "notes": description,
            "url": url,
            "extras": [{"key": "guid", "value": harvest_object.guid}],
        }

        if existing_dataset:
            package_schema = package_plugin.update_package_schema()
            context["schema"] = package_schema

            # Don't change the dataset identifier or name,
            # even if the title has changed.
            dataset["id"] = existing_dataset["id"]
            dataset["name"] = existing_dataset["name"]

            try:
                # Save reference to the package on the object
                harvest_object.package_id = dataset["id"]
                harvest_object.add()

                package_update = toolkit.get_action("package_update")
                package_update(context, dataset)
            except toolkit.ValidationError as err:
                self._save_object_error(
                    f"Package update validation error: {err.error_summary}",
                    harvest_object,
                    "Import",
                )
                return False
        else:
            harvest_source_pkg = model.Package.get(harvest_object.harvest_source_id)
            assert harvest_source_pkg is not None, (
                "Couldn't find HarvestSource object associated to current HarvestObject"
            )

            dataset["id"] = str(uuid.uuid4())
            dataset["name"] = name
            dataset["owner_org"] = harvest_source_pkg.owner_org

            package_schema = package_plugin.create_package_schema()
            package_schema["id"] = [unicode_safe]

            context["schema"] = package_schema

            try:
                # Save reference to the package on the object
                harvest_object.package_id = dataset["id"]
                harvest_object.add()

                # Defer constraints and flush so the dataset can be indexed with
                # the harvest object id (on the after_show hook from the harvester
                # plugin)
                model.Session.execute(
                    sa.text("SET CONSTRAINTS harvest_object_package_id_fkey DEFERRED")
                )
                model.Session.flush()

                package_create = toolkit.get_action("package_create")
                package_create(context, dataset)
            except toolkit.ValidationError as err:
                self._save_object_error(
                    f"Package create validation error: {err.error_summary}",
                    harvest_object,
                    "Import",
                )
                return False

        model.Session.commit()

        return True
