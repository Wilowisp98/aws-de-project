wanna talk about:
- the idea of the process as a whole
- the api, how it works, future improvements, like the payload division. doing a swagger documentation would be cool. talk about how to add new routes.
- the consolidation process using airflow, to grab the data that was being received via api.
- the terraform, which will prepare the environments.
- the docker file which will allow to have easy deploys.
- the reason to choose lambda over EC2 is that, this is an operation that would not have requests during the night, or, out of working hours, so EC2 would just be up spending resources and money, while lambda is only payed as its used.