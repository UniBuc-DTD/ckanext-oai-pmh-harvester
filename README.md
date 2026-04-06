[![CI](https://github.com/UniBuc-DTD/ckanext-oai-pmh-harvester/workflows/Continuous%20Integration/badge.svg?branch=main)](https://github.com/UniBuc-DTD/ckanext-oai-pmh-harvester/actions)

# Open Archives Initiative - Protocol for Metadata Harvesting (OAI-PMH) harvester plugin for CKAN

This repository contains a [CKAN](https://ckan.org/) plugin which can be used to pull records from an endpoint implementing the [Open Archives Initiative](https://www.openarchives.org/) [Protocol for Metadata Harvesting](https://www.openarchives.org/pmh/).

For example, the [Zenodo](https://zenodo.org/) data portal [supports this API](https://developers.zenodo.org/#oai-pmh) (and is our primary use case).

## Requirements

Requires the latest (stable) version of CKAN, which at the time of writing is CKAN 2.11.

## Installation

To install this extension:

1. Activate your CKAN virtual environment, for example:

   source /usr/lib/ckan/default/bin/activate

2. Clone the source and install it on the virtualenv

   git clone https://github.com/UniBuc-DTD/ckanext-oai-pmh-harvester.git
   cd ckanext-oai-pmh-harvester
   pip install -e .
   pip install -r requirements.txt

3. Add `oai_pmh_harvester` to the `ckan.plugins` setting in your CKAN
   config file (by default the config file is located at
   `/etc/ckan/default/ckan.ini`).

4. Restart CKAN. For example if you've deployed CKAN with NGINX on Ubuntu:

   sudo systemctl reload nginx

## Config settings

The extension adds a new type of harvest source, the "OAI-PMH" source.
You can create a new harvest sources which uses this type by using
the `ckanext-harvest` extension's web interface, as usual.

For example, to harvest datasets from Zenodo, set the source URL to `https://zenodo.org/oai2d`.
To restrict to entries from a given Zenodo community,
or to limit the number of datasets which will be retrieved,
add a JSON payload to the harvest source's configuration field:

```json
{
   "set": "user-<Zenodo community name>",
   "limit": 10
}
```


## Developer installation

To install `ckanext-oai-pmh-harvester` for development, activate your CKAN virtualenv and
do:

    git clone https://github.com/UniBuc-DTD/ckanext-oai-pmh-harvester.git
    cd ckanext-oai-pmh-harvester
    pip install -e .
    pip install -r dev-requirements.txt

## Tests

To run the tests, do:

    pytest --ckan-ini=test.ini

## License

[AGPL](https://www.gnu.org/licenses/agpl-3.0.en.html)
