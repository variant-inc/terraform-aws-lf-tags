# Terraform LF-tags module


## Input Variables
| Name     | Type    | Default   | Example     | Notes   |
| -------- | ------- | --------- | ----------- | ------- |
| assign | Map containing all the tags for assignement on Data Lake resources. | any |  |  |
| grants | Map containing all grants for tags in Data Lake. | any |  |  |

## Variable definitions

### assign
Contains all tag assignements.
```json
"assign": {<map of tag assignments>}
```

### grants
Contains all data lake permissions, grants, based on LF-tags.
Permissions:  'ALL'|'SELECT'|'ALTER'|'DROP'|'DELETE'|'INSERT'|'DESCRIBE'
Resource types: 'DATABASE'|'TABLE' (use table for column)
```json
"grants": {<map of tag grants>}
```

## Examples
### `main.tf`
```terarform
module "lf_tags" {
  source  = "github.com/variant-inc/terrafom-aws-lf-tags?ref=v1

  assign = var.assign
  grants = var.grants
}
```

### `grants.tfvars.json`
```json
{
  "grants": [
    {
      "principal": "arn:aws:iam::319244236588:role/service-role/luka-lambda-test-role-716xt5p3",
      "principal_grants": [
          {
            "resource_type": "DATABASE",
            "permissions": [
              "ALL"
            ],
            "permissions_with_grant_option": [

            ],
            "tags": {
              "owner": [
                "luka"
              ]
            }
          }
      ]
    }
  ]
}
```

### `assign.tfvars.json`
```json
{
  "assign": {
    "databases": [
      {
        "dbname": "test-db-luka",
        "tags": {
          "tag1": "value3",
          "tag2": "value2",
          "owner": "luka"
        },
        "tables": [
          {
            "tablename": "year_2021",
            "tags": {
              "role": "pu1",
              "owner": "hubert",
              "tag_table2": "value25"
            },
            "columns": [
              {
                "columnname": "firstname",
                "tags": {
                  "owner": "luka"
                },
                "columnname": "lastname",
                "tags": {
                  "owner": "luka"
                }
              }
            ]
          }
        ]
      },
      {
        "dbname": "db2",
        "tags": {
          "owner": "kuang"
        }
      }
    ]
  }
}
```

### `provider.tf`
```terraform
provider "aws" {
  region = "us-east-1"
}
```

### `variables.tf`
copy ones from module