
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_pull_request(pull_request,client):
    table = client.Table('pull-requests')
    item = pull_request.to_dict()
    item['id'] = pull_request.pr_id

    table.put_item(Item=item)
    #document = client.collection('pull-requests').document(pull_request.pr_id)
    #document.set(pull_request.to_dict())



def create_project(project,client):
    table = client.Table('projects')
    item = project.to_dict()
    item['id'] = project.name
    table.put_item(Item=item)
    #document = client.collection('projects').document(project.name)
    #document.set(project.to_dict())


def create_repository(repository,client):

    table = client.Table('repositories')
    item = repository.to_dict()


    item['id'] = repository.name
    table.put_item(Item=item)
    # logger.info(repository.name)
    #document = client.collection('repositories').document(repository.name)
    #document.set(repository.to_dict())


def create_comment(comment,client):
    table = client.Table('comments')
    item = comment.to_dict()
    item['id'] = comment.comment_id
    table.put_item(Item=item)


def create_user(item,client):
    table = client.Table('users')
    item['id'] = item.get('username')
    try:
        results = table.put_item(Item = item)
        return True
    except Exception as e:
        print(str(e))
        logger.error("Unable to create user due to exception")
        return False

def retrieve_filtered_records(query,client,table_name):
    table = client.Table(table_name)
    results = table.scan(FilterExpression= query)
    data = results.get('Items',[])
    while 'LastEvaluatedKey' in results:
        results = table.scan(
            FilterExpression=query,
            ExclusiveStartKey=results['LastEvaluatedKey']
        )
        data.extend(results.get('Items',[]))
    print(data)
    return data



