from pydoc import describe
import click
import pymongo
import pprint
import datetime


conn = pymongo.MongoClient('mongodb://44.193.181.226:27018') # modify this to be the host of your mongos shard cluster server.
db = conn.get_database('testdb')

@click.group()
def main():
    pass

@main.command()
@click.argument('asin')
@click.option('--stars', type=float, nargs=2, default=None, help='Filter reviews to specific star rating range (e.x. --stars 4 5 to get between 4-5 stars)')
@click.option('--count', '-c', default=10,help='Amount of reviews to show.')
@click.option('--search_text', '-t', default=None,help='Regular expression to search the review body with.')
@click.option('--frm', '--from',  type=click.DateTime(), default=None,help='Limit results to this starting posting date and time.')
@click.option('--to', type=click.DateTime(), default=None,help='Limit results to this ending posting date and time.')
def product_reviews(asin, stars, count, search_text, frm, to):
    '''Return product reviews for a specific product by its amazon identifier (asin).'''
    reviews = db.get_collection('reviews')
    query = {'asin': asin}

    if stars is not None:
        lo, hi = stars
        query['overall'] = {'$gte': lo, '$lte': hi}
    
    dates = {}
    if frm:
        dates['$gte'] = frm.timestamp()
    if to:
        dates['$lte'] = to.timestamp()
    if dates:
        query['unixReviewTime'] = dates

    if search_text is not None:
        query['reviewText'] = {'$regex': search_text}

    cur = reviews.find(query).limit(count)
    for res in cur:
        pprint.pprint(res)

@main.command()
@click.option('--id', '-i', default=None, help='Get user by their reviewer ID.')
@click.option('--name', '-n', default=None, help='Get user by their reviewer Name.')
@click.option('--stars', type=float, nargs=2, default=None, help='Filter reviews to specific star rating range (e.x. --stars 4 5 to get between 4-5 stars)')
@click.option('--count', '-c', default=10,help='Amount of reviews to show.')
@click.option('--frm', '--from', type=click.DateTime(), default=None,help='Limit results to this starting posting date and time.')
@click.option('--to', type=click.DateTime(), default=None,help='Limit results to this ending posting date and time.')
def inspect_reviewer(id, name, stars, count, frm, to):
    '''Return product reviews for a specific reviewer by their reviewer ID.'''
    reviews = db.get_collection('reviews')
    query = {}

    if stars is not None:
        lo, hi = stars
        query['overall'] = {'$gte': lo, '$lte': hi}

    dates = {}
    if frm:
        dates['$gte'] = frm.timestamp()
    if to:
        dates['$lte'] = to.timestamp()
    if dates:
        query['unixReviewTime'] = dates

    if id:
        query['reviewerID'] = id
    elif name:
        query['reviewerName'] = name
    else:
        print('Please specify either a reviewerID or name to search. See inspect-reviewer --help')
        return
    
    cur = reviews.find(query).limit(count)
    for res in cur:
        pprint.pprint(res)
        print()


@main.command()
@click.argument('unix_timestamp', type=float)
@click.option('--count', '-c', default=10,help='Amount of reviews to show.')
def reviews_at_unix_time(unix_timestamp, count):
    '''Get reviews posted at a specific UNIX epoch timestamp.'''
    reviews = db.get_collection('reviews')
    cur = reviews.find({'unixReviewTime': unix_timestamp}).limit(count)
    for res in cur:
        pprint.pprint(res)

@main.command()
@click.option('--reviewer_id', default=None, help='Get user by their reviewer ID.')
@click.option('--reviewer_name', default=None, help='Get user by their reviewer Name.')
@click.option('--asin', default=None, help='The amazon identified of the product to search.')
def average_rating(reviewer_id, reviewer_name, asin):
    '''Get reviews posted at a specific UNIX epoch timestamp.'''
    reviews = db.get_collection('reviews')
    matcher = {}
    if asin:
        matcher['asin'] = asin
    elif reviewer_id:
        matcher['reviewerID'] = reviewer_id
    elif reviewer_name:
        matcher['reviewerName'] = reviewer_name
    else:
        print('Please specify either a product asin, reviewerID or reviewer name to search. See inspect-reviewer --help')
        return
    cur = reviews.aggregate([
        {'$match': matcher},
        { '$group': { '_id': None, 'score':{'$avg':'$overall'} } }
    ])
    for res in cur:
        pprint.pprint(res['score'])


@main.command()
@click.option('--count', '-c', default=20,help='Amount of negative reviewers to show.')
def most_negative_reviewers(count):
    '''Get users giving the most negative reviews.'''
    reviews = db.get_collection('reviews')
    cur = reviews.aggregate([
        {'$match': {'overall':1}},
        { '$group': { '_id': '$reviewerID', 'reviewerName': {'$first': '$reviewerName'}, 'count': { '$sum': 1 } } },
        { '$sort': { 'count' : -1 } },
        { '$limit': count }
    ])
    for res in cur:
        pprint.pprint(res)

@main.command()
@click.argument('reviewer_id')
def rating_distribution_of(reviewer_id):
    '''Return distribution of ratings for a user, or @all for all reviews in the dataset.'''
    reviews = db.get_collection('reviews')

    matcher = {}
    if reviewer_id != '@all':
        matcher['reviewerID'] = reviewer_id
    cur = reviews.aggregate([
        {'$match': matcher},
        { '$group': { '_id': '$overall', 'count': { '$sum': 1 } } },
        { '$sort': { '_id' : -1 } }
    ])
    for res in cur:
        pprint.pprint(res)

@main.command()
@click.option('--id', '-i', default=None, help='Get user by their reviewer ID.')
@click.option('--name', '-n', default=None, help='Get user by their reviewer Name.')
@click.option('--stars', type=float, nargs=2, default=None, help='Filter reviews to specific star rating range (e.x. --stars 4 5 to get between 4-5 stars)')
@click.option('--frm', '--from', type=click.DateTime(), default=None,help='Limit results to this starting posting date and time.')
@click.option('--to', type=click.DateTime(), default=None,help='Limit results to this ending posting date and time.')
def review_count(id, name, stars, frm, to):
    '''Return product reviews for a specific reviewer by their reviewer ID.'''
    reviews = db.get_collection('reviews')
    query = {}

    if stars is not None:
        lo, hi = stars
        query['overall'] = {'$gte': lo, '$lte': hi}

    dates = {}
    if frm:
        dates['$gte'] = frm.timestamp()
    if to:
        dates['$lte'] = to.timestamp()
    if dates:
        query['unixReviewTime'] = dates

    if id:
        query['reviewerID'] = id
    elif name:
        query['reviewerName'] = name
    else:
        print('Please specify either a reviewerID or name to search. See inspect-reviewer --help')
        return
    
    count = reviews.count_documents(query)
    pprint.pprint(count)


if __name__ == '__main__':
    main()