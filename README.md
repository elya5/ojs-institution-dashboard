# OJS Insitution Dashboard

This streamlit dashboard shows information how researchers at an institution are using [OJS](https://pkp.sfu.ca/software/ojs/) journals.
The data comes from [OpenAlex](https://openalex.org/).

To run the project, first set the environment variables in `.streamlit/secrets.toml`:
```
DATAVERSE_API_KEY="<your_key>"
OPENALEX_API_KEY="<your_key>"
```

Then run with:
```
$ uv sync
$ uv run streamlit run dashboard.py
```
