# GitHub Action Observer

## dbt
To test the dbt parsing and event publishing you will need to install dbt.

For the latest go to the '[Install dbt](https://docs.getdbt.com/docs/core/installation)' page on the [getdbt.com](https://getdbt.com) website. [Installing with pip](https://docs.getdbt.com/docs/core/pip-install) is best, including the creation of the virtual environment. Hint: Use pip install dbt-postgres as this will install the adapter you need as well as dbt-core.

The Jaffle Shop example is the easiest quick start to follow, so you will need the[ PostGRES adapter](https://docs.getdbt.com/docs/core/pip-install#installing-the-adapter), too.

The general process is:
1. Install dbt
2. Install the PostGRES adapter
3. Clone the [jaffle_shop](https://github.com/dbt-labs/jaffle_shop) repo
4. Install a PostGRES docker container
5. Configure jaffle_shop
6. Run!

The parameters for the [dbt core agent](https://docs.datakitchen.io/articles/#!dataops-observability-help/configure-dbt-core-agent) are on the DataKitchen docs.

### dbt scenarios
To create an error in a dbt run, write a model that queries a table that doesn't exist. For example:
    `select * from bad_table`

To create a skipped model, refer (i.e. ref()) to a model that wasn't created. For example:
`select * from {{ ref('bad_table')}}`
