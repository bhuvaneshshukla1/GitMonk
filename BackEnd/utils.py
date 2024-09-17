from datetime import datetime

from models import Comment, PullRequestReview, PullRequestsPageInfo, PullRequestStatus, MergeableState, \
    PullRequest, RepositoryData, PullRequestStatusEnum, PullRequestMergeableEnum, TimeFrame, FilterCriteria

from db_client import (
    create_comment,
    create_repository,
    create_pull_request
)

from decimal import Decimal


def convert_float_to_decimal(value):
    return Decimal(str(value))



def map_comments(comments, pr_id, repo, project, client):
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
        create_comment(mapped_comment, client)
        mapped_comments.append(mapped_comment)
    return mapped_comments


def map_reviews(reviews, pr_id, repo, project,client):
    mapped_reviews = []
    for review in reviews:
        review_node = review.get('node', {})
        author = review_node.get('author', {}).get('login', None)
        review_comments = review_node.get('comments', {}).get('edges', [])
        mapped_review = PullRequestReview(map_comments(review_comments, pr_id, repo, project,client),
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
        'total_seconds': convert_float_to_decimal(time_difference.total_seconds())
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
        'total_seconds': convert_float_to_decimal(total_seconds // pr_count)
    }


def map_github_response_to_repository(github_repository_response, project, repo, client):
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
        mapped_comments = map_comments(comments, pr_id, repo, project,client)

        reviews = pull_request_node.get('reviews', {}).get('edges', [])
        mapped_reviews = map_reviews(reviews, pr_id, repo, project,client)
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
                            computeClosureTime(other_comment.created_date_time, comment.created_date_time)[
                                'total_seconds']
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
        create_pull_request(mapped_pull_request, client)
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
    create_repository(mapped_repository, client)
    return mapped_repository


def updatePullRequestStatusForProject(project_pr_status, response_pr_status):
    project_pr_status.open_state += response_pr_status.open_state
    project_pr_status.closed += response_pr_status.closed
    project_pr_status.merged += response_pr_status.merged


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
