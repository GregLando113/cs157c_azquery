from pydoc import describe
import click
import pymongo


conn = pymongo.MongoClient('mongodb://44.193.181.226:27018')
db = conn.get_database('testdb')

@click.group()
def main():
    pass

@main.command()
@click.argument('asin')
@click.option('--stars', type=float, nargs=2, default=None, help='Filter reviews to specific star rating range (e.x. --stars 4 5 to get between 4-5 stars)')
@click.option('--count', '-c', default=10,help='Amount of reviews to show.')
@click.option('--search_text', '-t', default=None,help='Regular expression to search the review body with.')
def product_reviews(asin, stars, count, search_text):
    '''Return product reviews for a specific product by its amazon identifier (asin).'''
    reviews = db.get_collection('reviews')
    query = {'asin': asin}

    if stars is not None:
        lo, hi = stars
        query['overall'] = {'$gte': lo, '$lte': hi}
    
    if search_text is not None:
        query['reviewText'] = {'$regex': search_text}

    cur = reviews.find(query).limit(count)
    for res in cur:
        print(res)

@main.command()
@click.option('--id', '-i', default=None, help='Get user by their reviewer ID.')
@click.option('--name', '-n', default=None, help='Get user by their reviewer Name.')
@click.option('--stars', type=float, nargs=2, default=None, help='Filter reviews to specific star rating range (e.x. --stars 4 5 to get between 4-5 stars)')
@click.option('--count', '-c', default=10,help='Amount of reviews to show.')
def inspect_reviewer(id, name, stars, count):
    '''Return product reviews for a specific reviewer by their reviewer ID.'''
    reviews = db.get_collection('reviews')
    query = {}

    if stars is not None:
        lo, hi = stars
        query['overall'] = {'$gte': lo, '$lte': hi}

    if id:
        query['reviewerID'] = id
    elif name:
        query['reviewerName'] = name
    else:
        print('Please specify either a reviewerID or name to search. See inspect-reviewer --help')
        return
    
    cur = reviews.find(query).limit(count)
    for res in cur:
        print(res)


@main.command()
@click.option('--count', '-c', default=20,help='Amount of negative reviewers to show.')
def most_negative_reviewers(count):
    '''Get distribution of users giving the most negative reviews.'''
    reviews = db.get_collection('reviews')
    cur = reviews.aggregate([
        {'$match': {'overall':1}},
        { '$group': { '_id': '$reviewerID', 'reviewerName': {'$first': '$reviewerName'}, 'count': { '$sum': 1 } } },
        { '$sort': { 'count' : -1 } },
        { '$limit': count }
    ])
    for res in cur:
        print(res)

@main.command()
@click.argument('reviewer_id')
def rating_distribution_of(reviewer_id):
    '''Return distribution of ratings for a user.'''
    reviews = db.get_collection('reviews')
    cur = reviews.aggregate([
        {'$match': {'reviewerID': reviewer_id}},
        { '$group': { '_id': '$overall', 'count': { '$sum': 1 } } },
        { '$sort': { '_id' : -1 } }
    ])
    for res in cur:
        print(res)


if __name__ == '__main__':
    main()