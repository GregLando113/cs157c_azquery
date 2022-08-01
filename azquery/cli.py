import click
import pymongo


conn = pymongo.MongoClient('mongodb://44.193.181.226:27018')
db = conn.get_database('testdb')

@click.group()
def main():
    pass

@main.command()
@click.argument('asin')
def product_reviews(asin):
    '''Return product reviews for a specific product by its amazon identifier (asin).'''
    reviews = db.get_collection('reviews')
    cur = reviews.find({'asin': asin})
    for res in cur:
        print(res)

@main.command()
@click.argument('reviewer_id')
def inspect_reviewer(reviewer_id):
    '''Return product reviews for a specific reviewer by their reviewer ID.'''
    reviews = db.get_collection('reviews')
    cur = reviews.find({'reviewerID': reviewer_id})
    for res in cur:
        print(res)



if __name__ == '__main__':
    main()