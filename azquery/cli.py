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
@click.option('--count',default=10,help='Amount of reviews to show.')
def product_reviews(asin, count):
    '''Return product reviews for a specific product by its amazon identifier (asin).'''
    reviews = db.get_collection('reviews')
    cur = reviews.find({'asin': asin}).limit(count)
    for res in cur:
        print(res)

@main.command()
@click.option('--id', default=None, help='Get user by their reviewer ID.')
@click.option('--name', default=None, help='Optionally get a user by their name over their ID.')
@click.option('--count',default=10,help='Amount of reviews to show.')
def inspect_reviewer(id, name, count):
    '''Return product reviews for a specific reviewer by their reviewer ID.'''
    reviews = db.get_collection('reviews')
    if id:
        cur = reviews.find({'reviewerID': id}).limit(count)
    elif name:
        cur = reviews.find({'reviewerName': name}).limit(count)
    else:
        print('Please specify either a reviewerID or name to search. See inspect-reviewer --help')
        return
    for res in cur:
        print(res)


@main.command()
@click.option('--count',default=20,help='Amount of negative reviewers to show.')
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