## Introduction
Welcome to Aspen Capital's Data Engineering challenge. This assignment will help us better assess your technical skills. We recommend that you focus on the requirements listed below and if time permitting - work on any additional features (of your own choosing).

## Background
We are in the process of migrating legacy databases to AWS. The legacy databases are located on prem in a colocation facility. We need to migrate the data in a cost effective way and be able to operate it with a small ops team. We need a pipeline that will sync the data to an AWS data lake and then ETL it into RDS to provide the data source for the new applications being built. While the migration is happening the AWS and on prem data need to stay in sync (some delay is allowed. Part of the submission is to decide what delay makes sense).

## Requirements
### High Level
*The solution needs to be provided as IaC for the infrastructure necessary for the pipeline
*Cost estimates need to be provided to show how much the solution will cost to operate (AWS cost calcuator is fine for this)

## Hints
* Using native managed AWS services is preferred over a lot of custom code.
* It is ok to choose any technology you like but you need to justify the choice.
* Do your best with the information available.

## Bonus Points
* Provision the project in AWS an provide access to review.
* Documentation - architecture diagrams, RFCs, etc.
* Alternative solution to this type of schema and a possible path to migrate it.

## Submission
* Your submission should be accessible in a public git repository that includes a README.md with all the information you want to include. The expectation is that we can easily follow the steps provided without much leg/guesswork.
* If your submission does include additional artifacts that are not represented within the repository - the README should provide information on how to retrieve and access these items.
