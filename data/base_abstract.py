import graphene


class QueriesAbstract(graphene.ObjectType):
    pass


class MutationsAbstract(graphene.ObjectType):
    pass


class Point(object):
    timestamp = graphene.Float(required=True)
    category = graphene.String()
    sensor = graphene.String()
    value = graphene.Float(required=True)
    unit = graphene.String()
