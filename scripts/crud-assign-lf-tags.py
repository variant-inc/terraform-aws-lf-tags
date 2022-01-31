import boto3
import json
import argparse

# get JSON string from terraform over parameter
parser = argparse.ArgumentParser(description='Get JSON with LF Tags.')
parser.add_argument('--data')
args = parser.parse_args()
data = json.loads(args.data)

# temp read from file for testing
# f = open("assign.auto.tfvars.json", "r")
# data = json.loads(f.read())['assign']

def order_values(tag):
    # sorts tag values for easier comparison
    tag['TagValues'] = sorted(tag['TagValues'])
    return tag

def order_keys(tags):
    # sorts tags by key in list of tags
    keys = sorted([i['TagKey'] for i in tags])
    tags_sorted = [i for key in keys for i in tags if i['TagKey'] == key]
    return tags_sorted

def get_glue_resource(resource_type, db_name='', table_name=''):
    if resource_type == 'db':
        list_identifier = 'DatabaseList'
        response = glue_client.get_databases()
    elif resource_type == 'table':
        list_identifier = 'TableList'
        response = glue_client.get_tables(
            DatabaseName = db_name
        )
    resources = []
    resources.append(response[list_identifier])
    while response[list_identifier] != []:
        next_token = response.get('NextToken', "")
        if next_token != "":
            if resource_type == 'db':
                response = glue_client.get_databases(
                    NextToken = next_token
                )
            elif resource_type == 'table':
                response = glue_client.get_tables(
                    NextToken = next_token,
                    DatabaseName = db_name
                )
            resources.append(response[list_identifier])
        else:
            break
    #flatten
    resources = [item for sublist in resources for item in sublist]
    return resources

def get_existing_tags(resource_type, db_name, table_name="", column_name=''):
    if resource_type == 'db':
        tags = get_db_tags(tag_type='existing', db_name=db_name)
    elif resource_type == 'table':
        tags = get_table_tags(tag_type='existing', db_name=db_name, table_name=table_name)
    elif resource_type == 'column':
        tags = get_column_tags(tag_type='existing', db_name=db_name, table_name=table_name, column_name=column_name)
    else:
        print('Invalid resource_type error.')
    return tags

def get_desired_tags(resource_type, db_name, table_name="", column_name='', db_index='', table_index='', column_index=''):
    if resource_type == 'db':
        tags = get_db_tags(tag_type='desired', db_name=db_name, db_index=db_index)
    elif resource_type == 'table':
        tags = get_table_tags(tag_type='desired', db_name=db_name, table_name=table_name, db_index=db_index, table_index=table_index)
    elif resource_type == 'column':
        tags = get_column_tags(tag_type='desired', db_name=db_name, table_name=table_name, column_name=column_name, db_index=db_index, table_index=table_index, column_index=column_index)
    else:
        print('Invalid resource_type error.')
    return tags

def get_db_tags(tag_type, db_name, db_index=''):
    if tag_type == 'existing':
        response = client.get_resource_lf_tags(
                Resource={
                    "Database": {
                        "Name": db_name
                    }
                }
            )
        existing_tags = response.get('LFTagOnDatabase', [])
        # remove CatalogId field
        for tag in existing_tags:
            del tag['CatalogId']
        # order values
        existing_tags = [order_values(i) for  i in existing_tags]
        existing_tags = order_keys(existing_tags)
        return existing_tags
    elif tag_type == 'desired':
        desired_tags_raw = data['databases'][db_index]['tags']
        desired_tags = []
        desired_keys = []
        for key in desired_tags_raw.keys():
            desired_keys.append(key)
            desired_tags.append({
                "TagKey": key,
                "TagValues": [desired_tags_raw[key]]
            })
        # order values
        desired_tags = [order_values(i) for  i in desired_tags]
        desired_tags = order_keys(desired_tags)
        return (desired_tags, desired_keys)
    else:
        print('Invalid tag_type error.')

def get_table_tags(tag_type, db_name, table_name, db_index='', table_index=''):
    if tag_type == 'existing':
        response = client.get_resource_lf_tags(
                Resource={
                    "Table": {
                        "DatabaseName": db_name,
                        "Name": table_name
                    }
                }
            )
        existing_tags = response.get('LFTagsOnTable', [])
        # remove CatalogId field
        for tag in existing_tags:
            del tag['CatalogId']
        # order values
        existing_tags = [order_values(i) for  i in existing_tags]
        existing_tags = order_keys(existing_tags)
        return existing_tags
    elif tag_type == 'desired':
        desired_tags_raw = data['databases'][db_index]['tags'] | data['databases'][db_index]['tables'][table_index]['tags']
        desired_tags = []
        desired_keys = []
        for key in desired_tags_raw.keys():
            desired_keys.append(key)
            desired_tags.append({
                "TagKey": key,
                "TagValues": [desired_tags_raw[key]]
            })
        # order values
        desired_tags = [order_values(i) for  i in desired_tags]
        desired_tags = order_keys(desired_tags)
        return (desired_tags, desired_keys)
    else:
        print('Invalid tag_type error.')

def get_column_tags(tag_type, db_name, table_name, column_name, db_index='', table_index='', column_index=''):
    if tag_type == 'existing':
        response = client.get_resource_lf_tags(
                Resource={
                    "TableWithColumns": {
                        "DatabaseName": db_name,
                        "Name": table_name,
                        "ColumnNames": [
                            column_name
                        ]
                    }
                }
            )
        existing_tags = response.get('LFTagsOnColumns', [])[0]['LFTags']
        # remove CatalogId field
        for tag in existing_tags:
            del tag['CatalogId']
        # order values
        existing_tags = [order_values(i) for  i in existing_tags]
        existing_tags = order_keys(existing_tags)
        return existing_tags
    elif tag_type == 'desired':
        desired_tags_raw = data['databases'][db_index]['tags'] | data['databases'][db_index]['tables'][table_index]['tags'] | data['databases'][db_index]['tables'][table_index]['columns'][column_index]['tags']
        desired_tags = []
        desired_keys = []
        for key in desired_tags_raw.keys():
            desired_keys.append(key)
            desired_tags.append({
                "TagKey": key,
                "TagValues": [desired_tags_raw[key]]
            })
        # order values
        desired_tags = [order_values(i) for  i in desired_tags]
        desired_tags = order_keys(desired_tags)
        return (desired_tags, desired_keys)
    else:
        print('Invalid tag_type error.')

def update_tags(resource_type, db_name, table_name='', column_name='', add=[], rm=[]):
    if resource_type == 'db':
        res = {
            "Database": {
                "Name": db_name
            }
        }
    elif resource_type == 'table':
        res = {
            "Table": {
                "DatabaseName": db_name,
                "Name": table_name
            }
        }
    elif resource_type == 'column':
        res = {
            "TableWithColumns": {
                "DatabaseName": db_name,
                "Name": table_name,
                "ColumnNames": [
                    column_name
                ]
            }
        }
    else:
        print("Invalid resource_type error.")
    add_response = client.add_lf_tags_to_resource(
        Resource = res,
        LFTags = add
    )
    if len(rm) > 0:
        remove_response = client.remove_lf_tags_from_resource(
            Resource = res,
            LFTags = rm
        )

# init of boto3 clients
client = boto3.client('lakeformation')
glue_client = boto3.client('glue')

# get existing LF tags
existing_tags = []
response_all = client.list_lf_tags()
existing_tags.append(response_all['LFTags'])
while response_all['LFTags'] != []:
    next_token = response_all.get('NextToken', "")
    if next_token != "":
        response_all = client.list_permissions(
            NextToken = next_token
        )
        existing_tags.append(response_all['LFTags'])
    else:
        break

# flatten existing permissions
existing_tags = [item for sublist in existing_tags for item in sublist]
# remove CatalogId field
for tag in existing_tags:
    del tag['CatalogId']

# order values
existing_tags = [order_values(i) for  i in existing_tags]

# transform input to match AWS format
desired_tags_raw = []
for db in data['databases']:
    desired_tags_raw.append(db['tags'])
    for table in db['tables']:
        desired_tags_raw.append(table['tags'])
        if table.get('columns', []) != []:
            for column in table['columns']:
                desired_tags_raw.append(column['tags'])

desired_tags = []
for item in desired_tags_raw:
    desired_keys = [i['TagKey'] for i in desired_tags]
    for key in item.keys():
        if key in desired_keys:
            index = next(i for i, item in enumerate(desired_tags) if item["TagKey"] == key)
            if item[key] in desired_tags[index]['TagValues']:
                pass
            else:
                desired_tags[index]['TagValues'].append(item[key])
        else:
            desired_tags.append({
                "TagKey": key,
                "TagValues": [item[key]]
            })

# order values
desired_tags = [order_values(i) for  i in desired_tags]

# create desired LF tags on AWS and update their possible values
existing_keys = [i['TagKey'] for i in existing_tags]
for tag in desired_tags:
    if tag['TagKey'] in existing_keys:
        index = next(i for i, item in enumerate(existing_tags) if item["TagKey"] == tag["TagKey"])
        if tag['TagValues'] == existing_tags[index]['TagValues']:
            pass
        else:
            # get values differences
            values_add = []
            values_delete = []
            for val in existing_tags[index]['TagValues']:
                if val in tag['TagValues']:
                    pass
                else:
                    values_delete.append(val)
            for val in tag['TagValues']:
                if val in existing_tags[index]['TagValues']:
                    pass
                else:
                    values_add.append(val)
            # update possible values
            if len(values_add) > 0 and len(values_delete) > 0:
                response = client.update_lf_tag(
                    TagKey = tag['TagKey'],
                    TagValuesToDelete = values_delete,
                    TagValuesToAdd = values_add
                )
            elif len(values_add) > 0 and len(values_delete) == 0:
                response = client.update_lf_tag(
                    TagKey = tag['TagKey'],
                    TagValuesToAdd = values_add
                )
            else:
                response = client.update_lf_tag(
                    TagKey = tag['TagKey'],
                    TagValuesToDelete = values_delete
                )
    else:
        # create tag with values
        print(f"Create {tag['TagKey']}")
        response = client.create_lf_tag(
            TagKey = tag['TagKey'],
            TagValues = tag['TagValues']
        )

# remove unwanted LF tags from AWS
for tag in existing_tags:
    if tag['TagKey'] in desired_keys:
        pass
    else:
        # delete tag
        response = client.delete_lf_tag(
            TagKey=tag['TagKey']
        )

# assign tags to resources
# get glue data catolog DBs
dbs = get_glue_resource(resource_type='db')

for db in dbs:
    # get existing tags
    existing_tags = get_existing_tags(resource_type='db', db_name=db['Name'])
    # get desired tags
    try:
        db_index = next(i for i, item in enumerate(data['databases']) if item['dbname'] == db['Name'])
    except StopIteration:
        print(f"DB {db['Name']} is not defined in JSON.")
        continue
    
    desired_tags, desired_keys = get_desired_tags(resource_type='db', db_name=db['Name'], table_name="", db_index=db_index)
    tags_to_remove = []
    if desired_tags == existing_tags:
        pass
    else:
        for et in existing_tags:
            if et['TagKey'] in desired_keys:
                pass
            else:
                tags_to_remove.append(et)
        update_tags(resource_type='db', db_name=db['Name'], add=desired_tags, rm=tags_to_remove)
        
    tables = get_glue_resource(resource_type='table', db_name=db['Name'])
    for table in tables:
        # get existing tags
        existing_tags = get_existing_tags(resource_type='table', db_name=db['Name'], table_name=table['Name'])
        # get desired tags
        try:
            table_index = next(i for i, item in enumerate(data['databases'][db_index]['tables']) if item['tablename'] == table['Name'])
        except StopIteration:
            print(f"Table {table['Name']} is not defined in JSON.")
            continue
        
        desired_tags, desired_keys = get_desired_tags(resource_type='table', db_name=db['Name'], table_name=table['Name'], db_index=db_index, table_index=table_index)
        tags_to_remove = []
        if desired_tags == existing_tags:
            pass
        else:
            for et in existing_tags:
                if et['TagKey'] in desired_keys:
                    pass
                else:
                    tags_to_remove.append(et)
            update_tags(resource_type='table', db_name=db['Name'], table_name=table['Name'], add=desired_tags, rm=tags_to_remove)

        columns = table['StorageDescriptor']['Columns']
        for column in columns:
            # get existing tags
            existing_tags = get_existing_tags(resource_type='column', db_name=db['Name'], table_name=table['Name'], column_name=column['Name'])
            # get desired tags
            try:
                column_index = next(i for i, item in enumerate(data['databases'][db_index]['tables'][table_index]['columns']) if item['columnname'] == column['Name'])
            except StopIteration:
                print(f"Column {column['Name']} is not defined in JSON.")
                continue
            
            desired_tags, desired_keys = get_desired_tags(resource_type='column', db_name=db['Name'], table_name=table['Name'], column_name=column['Name'], db_index=db_index, table_index=table_index, column_index=column_index)
            tags_to_remove = []
            if desired_tags == existing_tags:
                pass
            else:
                for et in existing_tags:
                    if et['TagKey'] in desired_keys:
                        pass
                    else:
                        tags_to_remove.append(et)
                update_tags(resource_type='column', db_name=db['Name'], table_name=table['Name'], column_name=column['Name'], add=desired_tags, rm=tags_to_remove)