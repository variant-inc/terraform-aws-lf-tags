import boto3
import json
import argparse

# get JSON string from terraform over parameter
parser = argparse.ArgumentParser(description='Get JSON with LF Grants.')
parser.add_argument('--data')
args = parser.parse_args()
data = json.loads(args.data)

def generate_resource(grant):
    # transforms resource policy from readable JSON to one in AWS format
    result_tags = []
    for key in grant['tags'].keys():
        result_tags.append({
            "TagKey": key,
            "TagValues": grant['tags'][key]
        })
    result = {
        "LFTagPolicy": {
            "CatalogId": account_id,
            "ResourceType": grant['resource_type'],
            "Expression": result_tags
        }
    }
    return result

def order_tags(grant):
    # sorts tag keys for easier comparison
    expression = grant['Resource']['LFTagPolicy']['Expression']
    tag_keys = sorted([i['TagKey'] for i in expression])
    ordered_expression = []
    for t in tag_keys:
        for e in expression:
            if t == e['TagKey']:
                ordered_expression.append(e)
    grant['Resource']['LFTagPolicy']['Expression'] = ordered_expression
    return grant

# init of lakeformatin clinet
client = boto3.client('lakeformation')
# get all current LF-Tag based permissions
existing_permissions = []
response_all = client.list_permissions(
    ResourceType='LF_TAG_POLICY',
)
existing_permissions.append(response_all['PrincipalResourcePermissions'])
next_token = response_all['NextToken']
while response_all['PrincipalResourcePermissions'] != []:
    response_all = client.list_permissions(
        ResourceType = 'LF_TAG_POLICY',
        NextToken = next_token
    )
    existing_permissions.append(response_all['PrincipalResourcePermissions'])

# flatten existing permissions
existing_permissions = [item for sublist in existing_permissions for item in sublist]
# order tags in existing permissions
ordered_permissions = []
for existing_grant in existing_permissions:
    ordered_permissions.append(order_tags(existing_grant))

# get acc id for catalog id field population
account_id = boto3.client('sts').get_caller_identity().get('Account')
all_transformed_grants = []
# transform all desired grants in AWS format for comparisson with current ones
for principal in data:
    for grant in principal['principal_grants']:
        transformed_resource = generate_resource(grant)
        transformed_grant = {
            "Principal": {
                "DataLakePrincipalIdentifier": principal['principal']
            },
            "Resource": transformed_resource,
            "Permissions": grant['permissions'],
            "PermissionsWithGrantOption": grant.get('permissions_with_grant_option', [])
        }
        transformed_grant = order_tags(transformed_grant)
        all_transformed_grants.append(transformed_grant)
        # check if it exists in current grants on LF
        if transformed_grant in ordered_permissions:
            pass
        else:
            #create grant
            print(f"Grant: {transformed_grant}")
            response = client.grant_permissions(
                Principal = transformed_grant['Principal'],
                Resource = transformed_grant['Resource'],
                Permissions = transformed_grant['Permissions'],
                PermissionsWithGrantOption = transformed_grant['PermissionsWithGrantOption']
            )

# check if any of existing permissions need to be deleted
for existing_grant in ordered_permissions:
    if existing_grant in all_transformed_grants:
        pass
    else:
        #revoke grant
        print(f"Revoke: {existing_grant}")
        response = client.revoke_permissions(
            Principal = existing_grant['Principal'],
            Resource = existing_grant['Resource'],
            Permissions = existing_grant['Permissions'],
            PermissionsWithGrantOption = existing_grant['PermissionsWithGrantOption']
        )