import requests
from flask import Flask, request, jsonify
import logging
from google.cloud import firestore
import os
from datetime import datetime
from typing import Optional
from enum import Enum
from flask_cors import CORS

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "datastore-access-key.json"
client = firestore.Client()
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
CORS(app, resources={r"/filterData": {"origins": "http://localhost:5173"},
                     r"/createUser": {"origins": "http://localhost:5173"},
                     r"/validUser": {"origins": "http://localhost:5173"}})


class MergeableState:
    def __init__(self, mergeable=0, conflicting=0, unknown=0):
        self.mergeable = mergeable
        self.conflicting = conflicting
        self.unknown = unknown

    def to_dict(self):
        return {
            'mergeable': self.mergeable,
            'conflicting': self.conflicting,
            'unknown': self.unknown
        }


class Project:
    def __init__(self, name=None, repositories=[], pr_status=None, total_comments_count=0, pull_requests_count=0,
                 mergeable_state=None, avg_comment_reply_time=None):
        self.name = name
        self.repositories = repositories
        self.pr_status = pr_status
        self.total_comments_count = total_comments_count
        self.pull_requests_count = pull_requests_count
        self.mergeable_state = mergeable_state
        self.avg_comment_reply_time = avg_comment_reply_time

    def to_dict(self):
        return {
            'name': self.name,
            'repositories': [repo for repo in self.repositories],
            'pr_status': self.pr_status.to_dict() if self.pr_status else None,
            'total_comments_count': self.total_comments_count,
            'pull_requests_count': self.pull_requests_count,
            'mergeable_state': self.mergeable_state.to_dict() if self.mergeable_state else None,
            'avg_comment_reply_time': self.avg_comment_reply_time
        }


class PullRequestReview:
    def __init__(self, comments=[], review_author=None, state=None):
        self.comments = comments
        self.review_author = review_author
        self.state = state

    def to_dict(self):
        return {
            'comments': [comment.to_dict() for comment in self.comments],
            'review_author': self.review_author,
            'state': self.state
        }


class Comment:
    def __init__(self, comment_id=None, comment_text=None, created_date_time=None, comment_author=None,
                 reply_to_comment_id=None, pull_request_id=None, repository=None, project=None):
        self.comment_id = comment_id
        self.comment_text = comment_text
        self.created_date_time = created_date_time
        self.comment_author = comment_author
        self.reply_to_comment_id = reply_to_comment_id
        self.pull_request_id = pull_request_id
        self.repository = repository
        self.project = project

    def to_dict(self):
        return {
            'comment_id': self.comment_id,
            'comment_text': self.comment_text,
            'created_date_time': self.created_date_time,
            'comment_author': self.comment_author,
            'reply_to_comment_id': self.reply_to_comment_id,
            'pull_request_id': self.pull_request_id,
            'repository': self.repository,
            'project': self.project

        }


class PullRequest:
    def __init__(self, pr_id=None, state=None, pull_request_number=None, title=None, is_mergeable=None,
                 total_comments_count=None, comments=[], reviews=[], author=None, project=None, repository=None,
                 createdAt=None, mergedAt=None, closedAt=None, closureTime=None, avg_comment_reply_time=None):
        self.pr_id = pr_id
        self.state = state
        self.pull_request_number = pull_request_number
        self.title = title
        self.is_mergeable = is_mergeable
        self.total_comments_count = total_comments_count
        self.comments = comments
        self.reviews = reviews
        self.author = author
        self.project = project
        self.repository = repository
        self.createdAt = createdAt
        self.mergedAt = mergedAt
        self.closedAt = closedAt
        self.closureTime = closureTime
        self.avg_comment_reply_time = avg_comment_reply_time

    def to_dict(self):
        return {
            'pr_id': self.pr_id,
            'state': self.state,
            'pull_request_number': self.pull_request_number,
            'title': self.title,
            'is_mergeable': self.is_mergeable,
            'total_comments_count': self.total_comments_count,
            'comments': [comment.to_dict() for comment in self.comments],
            'reviews': [review.to_dict() for review in self.reviews],
            'author': self.author,
            'project': self.project,
            'repository': self.repository,
            'createdAt': self.createdAt,
            'mergedAt': self.mergedAt,
            'closedAt': self.closedAt,
            'closureTime': self.closureTime,
            'avg_comment_reply_time': self.avg_comment_reply_time
        }


class PullRequestsPageInfo:
    def __init__(self, end_cursor=None, has_next_page=None, has_previous_page=None, created_date_time=None,
                 merged_date_time=None, closed_date_time=None):
        self.end_cursor = end_cursor
        self.has_next_page = has_next_page
        self.has_previous_page = has_previous_page
        self.created_date_time = created_date_time
        self.merged_date_time = merged_date_time
        self.closed_date_time = closed_date_time

    def to_dict(self):
        return {
            'end_cursor': self.end_cursor,
            'has_next_page': self.has_next_page,
            'has_previous_page': self.has_previous_page,
            'created_date_time': self.created_date_time,
            'merged_date_time': self.merged_date_time,
            'closed_date_time': self.closed_date_time
        }


class PullRequestStatus:
    def __init__(self, open_state=0, closed=0, merged=0):
        self.open_state = open_state
        self.closed = closed
        self.merged = merged

    def to_dict(self):
        return {
            'open': self.open_state,
            'closed': self.closed,
            'merged': self.merged
        }


class PullRequestStatusEnum(Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    MERGED = "MERGED"


class PullRequestMergeableEnum(Enum):
    MERGEABLE = "MERGEABLE"
    CONFLICTING = "CONFLICTING"
    UNKNOWN = "UNKNOWN"


class TimeFrame:
    def __init__(self, from_date: Optional[datetime] = None, to_date: Optional[datetime] = None):
        self.from_date = from_date
        self.to_date = to_date


class FilterCriteria:
    def __init__(self, status: Optional[PullRequestStatus] = None,
                 author: Optional[str] = None, timeframe: Optional[TimeFrame] = None, project: Optional[str] = None,
                 repository: Optional[str] = None, mergeable: Optional[PullRequestMergeableEnum] = None):
        self.status = status
        self.author = author
        self.timeframe = timeframe
        self.project = project
        self.repository = repository
        self.mergeable = mergeable


class RepositoryData:
    def __init__(self, name=None, pull_requests_count=None, total_comments_count=None, pr_status=None,
                 average_closure_time=None, mergeable_state=None, avg_comment_reply_time=None):
        self.name = name
        self.pull_requests_count = pull_requests_count
        self.total_comments_count = total_comments_count
        self.pr_status = pr_status
        self.average_closure_time = average_closure_time
        self.mergeable_state = mergeable_state
        self.avg_comment_reply_time = avg_comment_reply_time

    def to_dict(self):
        return {
            'name': self.name,
            'pull_requests_count': self.pull_requests_count,
            'total_comments_count': self.total_comments_count,
            'pr_status': self.pr_status.to_dict() if self.pr_status else None,
            'average_closure_time': self.average_closure_time,
            'mergeable_state': self.mergeable_state.to_dict() if self.mergeable_state else None,
            'avg_comment_reply_time': self.avg_comment_reply_time
        }


def map_comments(comments, pr_id, repo, project):
    mapped_comments = []

    for comment in comments:
        comment_node = comment.get('node', {})
        mapped_comment = Comment(comment_node.get('id', None),
                                 comment_node.get('body', None),
                                 comment_node.get('createdAt', None),
                                 comment_node.get('author', {}).get('login', None),
                                 comment_node.get('replyTo', {}),
                                 pr_id,
                                 repo,
                                 project)
        create_comment(mapped_comment)
        mapped_comments.append(mapped_comment)
    return mapped_comments


def map_reviews(reviews, pr_id, repo, project):
    mapped_reviews = []
    for review in reviews:
        review_node = review.get('node', {})
        author = review_node.get('author', {}).get('login', None)
        review_comments = review_node.get('comments', {}).get('edges', [])
        mapped_review = PullRequestReview(map_comments(review_comments, pr_id, repo, project),
                                          author,
                                          review_node.get('state', None))
        mapped_reviews.append(mapped_review)
    return mapped_reviews


def updatePRStatustracker(pull_request_status, status):
    if status == 'OPEN':
        pull_request_status.open_state += 1
    elif status == 'CLOSED':
        pull_request_status.closed += 1
    elif status == 'MERGED':
        pull_request_status.merged += 1


def updateMergeableStateTracker(mergeable_state, pull_request_mergeable_state):
    if pull_request_mergeable_state == 'MERGEABLE':
        mergeable_state.mergeable += 1
    elif pull_request_mergeable_state == 'CONFLICTING':
        mergeable_state.conflicting += 1
    elif pull_request_mergeable_state == 'UNKNOWN':
        mergeable_state.unknown += 1


def updateMergeableStateTrackerForProject(mergeable_state, response_mergeable_state):
    mergeable_state.mergeable += response_mergeable_state.mergeable
    mergeable_state.conflicting += response_mergeable_state.conflicting
    mergeable_state.unknown += response_mergeable_state.unknown


def computeClosureTime(start_time, end_time):
    date_format = "%Y-%m-%dT%H:%M:%SZ"

    created_at = datetime.strptime(start_time, date_format)
    concluded_at = datetime.strptime(end_time, date_format)

    time_difference = concluded_at - created_at

    return {
        'days': time_difference.days,
        'hours': time_difference.seconds // 3600,
        'minutes': (time_difference.seconds % 3600) // 60,
        'total_seconds': time_difference.total_seconds()
    }


def compute_average_closure_time(total_seconds, pr_count):
    if pr_count == 0:
        return None
    seconds = total_seconds // pr_count
    days = seconds // (24 * 3600)
    seconds %= (24 * 3600)
    hours = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60

    return {
        'days': days,
        'hours': hours,
        'minutes': minutes,
        'total_seconds': total_seconds // pr_count
    }


def map_github_response_to_repository(github_repository_response, project, repo):
    repository_data = github_repository_response.get('data', {}).get('repository', {})
    page_info_data = repository_data.get('pullRequests', {}).get('pageInfo', {})
    mapped_page_info = PullRequestsPageInfo(page_info_data.get('endCursor', None),
                                            page_info_data.get('hasNextPage', None),
                                            page_info_data.get('hasPreviousPage', None))
    pull_requests_count = repository_data.get('pullRequests', {}).get('totalCount', None)
    pull_requests = repository_data.get('pullRequests', {}).get('edges', [])

    mapped_pull_requests = []
    total_comments_count = 0
    concluded_pr_count = 0
    total_open_time = 0
    pull_request_status = PullRequestStatus()
    mergeable_state = MergeableState()
    repo_time_taken_to_reply = 0
    repo_comment_reply_count = 0
    for pull_request in pull_requests:
        pull_request_node = pull_request.get('node', {})
        author = pull_request_node.get('author', {}).get('login', None)
        pr_id = pull_request_node.get('id', None)

        comments = pull_request_node.get('comments', {}).get('edges', [])
        mapped_comments = map_comments(comments, pr_id, repo, project)

        reviews = pull_request_node.get('reviews', {}).get('edges', [])
        mapped_reviews = map_reviews(reviews, pr_id, repo, project)
        closure_time = None

        create_time = pull_request_node.get('createdAt', None)
        merged_time = pull_request_node.get('mergedAt', None)
        closed_time = pull_request_node.get('closedAt', None)
        if pull_request_node.get('closed', False) == True:
            closure_time = computeClosureTime(create_time, closed_time)
            concluded_pr_count += 1
            total_open_time += closure_time['total_seconds']
        elif pull_request_node.get('merged', False) == True:
            closure_time = computeClosureTime(create_time, merged_time)
            concluded_pr_count += 1
            total_open_time += closure_time['total_seconds']

        all_comments = mapped_comments
        for review in mapped_reviews:
            all_comments.extend(review.comments)

        time_taken_to_reply = 0
        comment_reply_count = 0
        for comment in all_comments:
            if comment.reply_to_comment_id is not None and comment.reply_to_comment_id.get('id', None) is not None:
                comment_reply_count += 1
                for other_comment in all_comments:
                    if other_comment.comment_id == comment.reply_to_comment_id.get('id', None):
                        time_taken_to_reply += \
                        computeClosureTime(other_comment.created_date_time, comment.created_date_time)['total_seconds']
                        break

        average_turnaround_time = compute_average_closure_time(time_taken_to_reply, comment_reply_count)
        repo_time_taken_to_reply += time_taken_to_reply
        repo_comment_reply_count += comment_reply_count

        mapped_pull_request = PullRequest(pr_id,
                                          pull_request_node.get('state', None),
                                          pull_request_node.get('number', None),
                                          pull_request_node.get('title', None),
                                          pull_request_node.get('mergeable', None),
                                          pull_request_node.get('totalCommentsCount', None),
                                          mapped_comments,
                                          mapped_reviews,
                                          author,
                                          project,
                                          repo,
                                          create_time,
                                          merged_time,
                                          closed_time,
                                          closure_time,
                                          average_turnaround_time
                                          )
        create_pull_request(mapped_pull_request)
        mapped_pull_requests.append(mapped_pull_request)
        total_comments_count += int(pull_request_node.get('totalCommentsCount', 0))
        updatePRStatustracker(pull_request_status, pull_request_node.get('state', None))
        updateMergeableStateTracker(mergeable_state, pull_request_node.get('mergeable', None))

    average_closure_time = compute_average_closure_time(total_open_time, concluded_pr_count)
    average_turnaround_time_repo = compute_average_closure_time(repo_time_taken_to_reply, repo_comment_reply_count)
    mapped_repository = RepositoryData(repository_data.get('name', None),
                                       pull_requests_count,
                                       total_comments_count,
                                       pull_request_status,
                                       average_closure_time,
                                       mergeable_state,
                                       average_turnaround_time_repo)
    create_repository(mapped_repository)
    return mapped_repository


def updatePullRequestStatusForProject(project_pr_status, response_pr_status):
    project_pr_status.open_state += response_pr_status.open_state
    project_pr_status.closed += response_pr_status.closed
    project_pr_status.merged += response_pr_status.merged


def create_pull_request(pull_request):
    document = client.collection('pull-requests').document(pull_request.pr_id)
    document.set(pull_request.to_dict())


def create_project(project):
    document = client.collection('projects').document(project.name)
    document.set(project.to_dict())


def create_repository(repository):
    # logger.info(repository.name)
    document = client.collection('repositories').document(repository.name)
    document.set(repository.to_dict())


def create_comment(comment):
    document = client.collection('comments').document(comment.comment_id)
    document.set(comment.to_dict())


# PROJECT_REPO_MAPPINGS = {'apache':['kafka'],'bhuvaneshshukla1':['mp2']}
PROJECT_REPO_MAPPINGS = {'apache': ['kafka', 'jmeter'], 'bhuvaneshshukla1': ['mp2']}

# USER_PROJECT_ROLE_MAPPINGS =  {'bhuvaneshshukla1': [{'project': 'apache','role': ['Manager']},{'project': 'bhuvaneshshukla1','role':['Developer','Manager','Reviewer']}]}
USER_PROJECT_ROLE_MAPPINGS = {
    'bhuvaneshshukla1': [{'project': 'bhuvaneshshukla1', 'role': ['Developer', 'Manager', 'Reviewer']},
                         {'project': 'apache', 'role': ['Manager']}],
    'bhuvi1996': [{'project': 'bhuvaneshshukla1', 'role': ['Developer', 'Manager', 'Reviewer']}]}


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
                'Authorization': 'Bearer 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',
                'Content-Type': 'application/json'
            }
            response = requests.post('https://api.github.com/graphql', headers=headers,
                                     json={'query': query, 'variables': variables}).json()

            mapped_response = map_github_response_to_repository(response, project, repo)
            project_object.pull_requests_count += mapped_response.pull_requests_count
            project_object.repositories.append(repo)
            project_object.total_comments_count += mapped_response.total_comments_count

            updatePullRequestStatusForProject(project_object.pr_status, mapped_response.pr_status)
            updateMergeableStateTrackerForProject(project_object.mergeable_state, mapped_response.mergeable_state)

            if mapped_response.avg_comment_reply_time is not None:
                avg_comment_reply_time += mapped_response.avg_comment_reply_time.get('total_seconds')
                repo_counter += 1

        project_object.avg_comment_reply_time = compute_average_closure_time(avg_comment_reply_time, repo_counter)
        create_project(project_object)

    return jsonify({"result": "success"}), 200


def constructFilterCriteria(requested_data):
    status = requested_data.get('status')
    author = requested_data.get('author')
    from_date = requested_data.get('from_date')
    to_date = requested_data.get('to_date')
    project = requested_data.get('project')
    repository = requested_data.get('repository')
    mergeable = requested_data.get('mergeable_state')

    status = PullRequestStatusEnum[status] if status else None
    mergeable = PullRequestMergeableEnum[mergeable] if mergeable else None
    # from_date = from_date if from_date else None
    # to_date = to_date if to_date else None

    time_frame = None
    if from_date is not None or to_date is not None:
        time_frame = TimeFrame(from_date, to_date)

    return FilterCriteria(status=status, author=author, timeframe=time_frame, project=project, repository=repository,
                          mergeable=mergeable)


@app.route('/filterData', methods=['POST'])
def filterData():
    requested_data = request.get_json()
    applied_filters = constructFilterCriteria(requested_data)
    query = client.collection('pull-requests')
    if applied_filters.author:
        query = query.where('author', '==', applied_filters.author)
    if applied_filters.repository:
        query = query.where('repository', '==', applied_filters.repository)
    if applied_filters.project:
        query = query.where('project', '==', applied_filters.project)
    if applied_filters.status:
        query = query.where('state', '==', applied_filters.status.value)
    if applied_filters.mergeable:
        query = query.where('is_mergeable', '==', applied_filters.mergeable.value)
    if applied_filters.timeframe:
        if applied_filters.timeframe.to_date:
            query = query.where('createdAt', '>=', applied_filters.timeframe.from_date)
            query = query.where('createdAt', '<=', applied_filters.timeframe.to_date)
        else:
            query = query.where('createdAt', '>=', applied_filters.timeframe.from_date)
    results = query.stream()

    pull_requests = [doc.to_dict() for doc in results]

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
    query = client.collection('users')
    query = query.where('username', '==', requested_data.get('username'))
    results = query.stream()
    if any(True for doc in results):
        logger.info("user already exisis")
        return {"result": "user already exists"}, 200
    collection = client.collection("users")
    new_doc_reference = collection.document()
    data = {
        "username": requested_data.get('username'),
        "password": requested_data.get('password')
    }
    new_doc_reference.set(data)
    return {"result": "success"}, 200


@app.route('/validUser', methods=['POST'])
def validUser():
    requested_data = request.get_json()
    logger.info(requested_data.get('username'))
    query = client.collection('users')
    query = query.where('username', '==', requested_data.get('username'))
    query = query.where('password', '==', requested_data.get('password'))
    results = query.stream()
    if any(True for doc in results):
        return {"result": "success"}, 200

    return {"result": "invalid credentials"}, 200


if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)
