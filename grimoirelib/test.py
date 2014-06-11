#! /usr/bin/python


from sqlalchemy import create_engine, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import ForeignKeyConstraint

engine = create_engine('mysql://jgb:XXX@localhost/vizgrimoire_cvsanaly',
                       encoding='utf8', echo=True)
Base = declarative_base(engine)

class SCMLog(Base):
    """ """
    __tablename__ = 'scmlog'
    __table_args__ = {'autoload':True}

class Actions(Base):
    """ """
    __tablename__ = 'actions'
    __table_args__ = (
        ForeignKeyConstraint(['commit_id'], ['scmlog.id']),
        {'autoload':True}
        )
    
def loadSession():
    """ """
#    metadata = Base.metadata
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

# class VarType:
#     """Class for specifying the kind of variable

#     Kinds of variables of interest are aggregate, count, list,
#     time series...
#     """

#     def selector(self, field):
#         return (selector)

#     def __init__(self, selector):
#         self.selector = selector

# VarTypeCount = VarType (func.count)

# def getCommitsQuery (type, start, end):
#     """
#     Get commits

#     Get all commits between starting date and end date
#       (exactly: start <= date < end
#     - selected: what is in the SELECt statement for the query,
#         such as func.count(func.distinct(SCMLog.id)
#     - start: string, starting date, such as "2013-06-01"
#     - end: string, end date, such as "2014-01-01"
#     """

#     res = session.query(type.selector(func.distinct(SCMLog.id))).\
#         join(Actions).\
#         filter(SCMLog.date >= start).\
#         filter(SCMLog.date < end)
#     return res

def getNCommitsQuery (start, end):
    """
    Get commits

    Get all commits between starting date and end date
      (exactly: start <= date < end
    - start: string, starting date, such as "2013-06-01"
    - end: string, end date, such as "2014-01-01"

# SELECT count(distinct(s.id)) as commits
# FROM scmlog s, actions a
# WHERE s.date >= "2013-06-01" and
# 	s.date < "2014-01-01" and 
# 	s.id = a.commit_id
"""

    res = session.query(func.count(func.distinct(SCMLog.id))).\
        join(Actions).\
        filter(SCMLog.date >= start).\
        filter(SCMLog.date < end)
    return res

def getCommitsQuery (start, end):
    """
    Get commits

    Get all commits between starting date and end date
      (exactly: start <= date < end
    - start: string, starting date, such as "2013-06-01"
    - end: string, end date, such as "2014-01-01"
"""

    res = session.query(func.distinct(SCMLog.id).label("id"), SCMLog.date).\
        join(Actions).\
        filter(SCMLog.date >= start).\
        filter(SCMLog.date < end)
    return res

if __name__ == "__main__":
    session = loadSession()
    res = session.query(SCMLog).first()
    print res.id
    res = session.query(Actions.type, SCMLog.rev).distinct(SCMLog.id).join(SCMLog)
    print res
    print
    print
    res = session.query(Actions.id).distinct(Actions.type)
    print res.first()

    # print "Other"
    # print
    # res = getCommitsQuery(selected=func.count(func.distinct(SCMLog.id)),
    #                       start="2013-06-01", end="2014-01-01")
    # print res.first()

    print "Other"
    print
#    res = getCommitsQuery(selected=func.count(SCMLog.id),
#                          start="2013-06-01", end="2014-01-01")
#    res = getCommitsQuery(type=VarTypeCount,
#                          start="2013-06-01", end="2014-01-01")
    res = getNCommitsQuery(start="2013-06-01", end="2014-01-01")
    print res.scalar()
    res = getCommitsQuery(start="2013-06-01", end="2014-01-01")
    print res.column_descriptions
    for row in res.limit(10).all():
        print row.id, row.date
