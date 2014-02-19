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

def getCommitsQuery (start, end):
    """
    Get commits

    Get all commits between starting date and end date
      (exactly: start <= date < end
    - Start: string, starting date, such as "2013-06-01"
    - End: string, end date, such as "2014-01-01"

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
    
    res = getCommitsQuery(start="2013-06-01", end="2014-01-01")
    print res.first()
