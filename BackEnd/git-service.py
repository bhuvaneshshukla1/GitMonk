import requests
import json
from flask import Flask, request, jsonify
import logging
from db_client import create_project, create_user, retrieve_filtered_records
from utils import (
    map_github_response_to_repository,
    updatePullRequestStatusForProject,
    updateMergeableStateTrackerForProject,
    compute_average_closure_time,
    constructFilterCriteria,
    updatePRStatustracker,
    updateMergeableStateTracker
)
from models import (
    MergeableState,
    Project,
    PullRequestStatus
)
from flask_cors import CORS
from boto3.dynamodb.conditions import Attr
app = Flask(__name__)
import boto3
from botocore.exceptions import ClientError


def get_secret():
    secret_name = "github_token"
    region_name = "us-east-1"
    # Create a Secrets Manager client
    session = boto3.session.Session()
    boto_client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = boto_client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e
    secret = json.loads(get_secret_value_response['SecretString'])
    return secret['github_token']

github_token = get_secret()
print(github_token)

# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "datastore-access-key.json"
# client = firestore.Client()

#aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
#aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
aws_region = 'us-east-1'

client = boto3.resource('dynamodb',
                        #aws_access_key_id=aws_access_key_id,
                        #aws_secret_access_key=aws_secret_access_key,
                        region_name=aws_region)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
CORS(app, resources={r"/filterData": {"origins": "http://ac7cf593349294ea2a773107664787f1-221083915.us-east-1.elb.amazonaws.com"},
                     r"/createUser": {"origins": "http://ac7cf593349294ea2a773107664787f1-221083915.us-east-1.elb.amazonaws.com"},
                     r"/validUser": {"origins": "http://ac7cf593349294ea2a773107664787f1-221083915.us-east-1.elb.amazonaws.com"}})

PROJECT_REPO_MAPPINGS = {'apache': ['kafka', 'jmeter'], 'bhuvaneshshukla1': ['mp2']}
@app.route('/')
def home():
    return jsonify({'message': 'API is working'}), 200


@app.route('/runCronJob', methods=['POST'])
def fetch():
    for project, repositories in PROJECT_REPO_MAPPINGS.items():
        project_object = Project(project, pr_status=PullRequestStatus(), mergeable_state=MergeableState())
        avg_comment_reply_time = 0
        repo_counter = 0
        for repo in repositories:
            query = """
            query($owner: String!, $name: String!, $pullRequestCount: Int!) {
              repository(owner: $owner, name: $name) {
                primaryLanguage {
                  name
                }
                description
                name
                watchers {
                  totalCount
                }
                pullRequests(last: $pullRequestCount) {
                  pageInfo {
                    endCursor
                    hasNextPage
                    hasPreviousPage
                  }
                  totalCount
                  edges {
                    cursor
                    node {
                      ... on PullRequest {
                        id
                        reviewDecision
                        state
                        number
                        title
                        author {
                          login
                        }
                        createdAt
                        mergedAt
                        closedAt
                        closed
                        url
                        changedFiles
                        additions
                        deletions
                        mergeable
                        totalCommentsCount
                        comments(last: 20) {
                          edges {
                            node {
                              createdAt
                              body
                              author {
                                login
                              }
                              id
                            }
                          }
                        }
                        reviews(last: 20) {
                          edges {
                            node {
                              state
                              author {
                                login
                              }
                              comments(last: 20) {
                                edges {
                                  node {
                                    id
                                    createdAt
                                    body
                                    author {
                                      login
                                    }
                                    replyTo {
                                      id
                                    }
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
            """

            variables = {
                "owner": project,
                "name": repo,
                "pullRequestCount": 10
            }
            headers = {
                'Authorization': f'Bearer {github_token}',
                'Content-Type': 'application/json'
            }
            response = requests.post('https://api.github.com/graphql', headers=headers,
                                     json={'query': query, 'variables': variables}).json()

            mapped_response = map_github_response_to_repository(response, project, repo, client)

            project_object.pull_requests_count += mapped_response.pull_requests_count
            project_object.repositories.append(repo)
            project_object.total_comments_count += mapped_response.total_comments_count

            updatePullRequestStatusForProject(project_object.pr_status, mapped_response.pr_status)
            updateMergeableStateTrackerForProject(project_object.mergeable_state, mapped_response.mergeable_state)

            if mapped_response.avg_comment_reply_time is not None:
                avg_comment_reply_time += mapped_response.avg_comment_reply_time.get('total_seconds')
                repo_counter += 1

        project_object.avg_comment_reply_time = compute_average_closure_time(avg_comment_reply_time, repo_counter)
        create_project(project_object, client)

    return jsonify({"result": "success"}), 200


def build_query(applied_filters):
    filter_expression = None
    if applied_filters.author:
        expr = Attr('author').eq(applied_filters.author)
        filter_expression = expr if not filter_expression else filter_expression & expr
    if applied_filters.repository:
        expr = Attr('repository').eq(applied_filters.repository)
        filter_expression = expr if not filter_expression else filter_expression & expr

    if applied_filters.project:
        expr = Attr('project').eq(applied_filters.project)
        filter_expression = expr if not filter_expression else filter_expression & expr

    if applied_filters.status:
        expr = Attr('status').eq(applied_filters.status.value)
        filter_expression = expr if not filter_expression else filter_expression & expr

    if applied_filters.mergeable:
        expr = Attr('is_mergeable').eq(applied_filters.mergeable.value)
        filter_expression = expr if not filter_expression else filter_expression & expr

    if applied_filters.timeframe:
        if applied_filters.timeframe.to_date:
            expr = Attr('createdAt').gte(applied_filters.timeframe.from_date)
            filter_expression = expr if not filter_expression else filter_expression & expr
            expr = Attr('createdAt').lte(applied_filters.timeframe.to_date)
            filter_expression = expr if not filter_expression else filter_expression & expr
        else:
            expr = Attr('createdAt').gte(applied_filters.timeframe.from_date)
            filter_expression = expr if not filter_expression else filter_expression & expr

    return filter_expression


@app.route('/filterData', methods=['POST'])
def filterData():
    requested_data = request.get_json()
    applied_filters = constructFilterCriteria(requested_data)
    query = build_query(applied_filters)
    pull_requests = retrieve_filtered_records(query, client, 'pull-requests')
    average_turnaround_time_per_comment = 0
    pull_requests_status = PullRequestStatus()
    pull_requests_mergeable = MergeableState()
    avg_time_from_create_to_conclude = 0
    concluded_pr_count = 0
    pr_with_replies_count = 0
    total_comments = 0
    pull_request_count = 0
    for pull_request in pull_requests:
        total_comments += pull_request.get('total_comments_count', 0)
        if pull_request.get('avg_comment_reply_time') is not None and pull_request.get('avg_comment_reply_time',
                                                                                       {}).get('total_seconds',
                                                                                               None) is not None:
            average_turnaround_time_per_comment += pull_request.get('avg_comment_reply_time', {}).get('total_seconds',
                                                                                                      None)
            pr_with_replies_count += 1
        if pull_request.get('state') is not None:
            updatePRStatustracker(pull_requests_status, pull_request.get('state'))
        if pull_request.get('is_mergeable') is not None:
            updateMergeableStateTracker(pull_requests_mergeable, pull_request.get('is_mergeable'))
        if pull_request.get('closureTime') is not None and pull_request.get('closureTime', {}).get('total_seconds',
                                                                                                   None) is not None:
            avg_time_from_create_to_conclude += pull_request.get('closureTime', {}).get('total_seconds', None)
            concluded_pr_count += 1
        pull_request_count += 1
    comment_turnaround_metric = compute_average_closure_time(average_turnaround_time_per_comment, pr_with_replies_count)
    pr_conclusion_time_metric = compute_average_closure_time(avg_time_from_create_to_conclude, concluded_pr_count)
    logger.info(comment_turnaround_metric)
    logger.info(pr_conclusion_time_metric)
    logger.info(pull_requests_status.to_dict())
    logger.info(total_comments)
    logger.info(pull_requests_mergeable.to_dict())
    return jsonify({"avg_comment_turnaround_time": comment_turnaround_metric,
                    "avg_pull_request_closure_time": pr_conclusion_time_metric,
                    "pull_request_status": pull_requests_status.to_dict(),
                    "total_comments": total_comments,
                    "pull_request_merge_status": pull_requests_mergeable.to_dict(),
                    "pull_request_count": pull_request_count}), 200


@app.route('/createUser', methods=['POST'])
def createUser():
    requested_data = request.get_json()
    logger.info(requested_data.get('username'))
    item = {
        'username': requested_data.get('username'),
        'password': requested_data.get('password')
    }
    if create_user(item, client):
        return {"result": "success"}, 200
    return {"result": "failure"}, 500


@app.route('/validUser', methods=['POST'])
def validUser():
    requested_data = request.get_json()
    logger.info(requested_data.get('username'))
    query = Attr('username').eq(requested_data.get('username')) & Attr('password').eq(requested_data.get('password'))
    results = retrieve_filtered_records(query, client, 'users')
    if len(results) >= 1:
        return {"result": "success"}, 200
    return {"result": "invalid credentials"}, 200


if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)
