from datetime import datetime
from enum import Enum
from typing import Optional


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
