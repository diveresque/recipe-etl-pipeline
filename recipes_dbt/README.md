# dbt modelling

## Requirements


- Load data to Google BigQuery (see main README)
- Install dbt
- Update profile.yml file in your local dbt installation to include database details

```yml
recipes_dbt:
  outputs:
    dev:
      dataset: recipe_dw
      job_execution_timeout_seconds: 300
      job_retries: 1
      location: <LOCATION>
      method: oauth
      priority: interactive
      project: <PROJECT_NAME>
      threads: 4
      type: bigquery
  target: dev
```

## To run

- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
