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
    
# SELECT count(distinct(s.id)) as commits
# FROM scmlog s, actions a
# WHERE s.date >= "2013-06-01" and
# 	s.date < "2014-01-01" and 
# 	s.id = a.commit_id

    print 
    print "Next one"
    print
    res = session.query(func.count(func.distinct(SCMLog.id))).\
        join(Actions).\
        filter(SCMLog.date >= "2013-06-01").\
        filter(SCMLog.date < "2014-01-01")
    print res.first()

# from sqlalchemy import MetaData
# from sqlalchemy import orm

# class SCMLog (object):
#     pass
# class Actions (object):
#     pass

# meta = MetaData(bind=engine, reflect=True)
# orm.mapper(SCMLog, meta.tables["scmlog"])
# orm.mapper(Actions, meta.tables["actions"])
# session = orm.Session (bind=engine)
# q = session.query(SCMLog.id, SCMLog.date).filter(SCMLog.id == 2).first()
# print (q)


# Base = declarative_base()
# class Commit(Base):
#     __tablename__ = 'scmlog'

#     id = Column(Integer, primary_key=True, display_width=11)
#     rev = Column(MEDIUMTEXT)
#     committer_id = Column(Integer, key=True, display_width=11)
#     author_id = Column(Integer, key=True, display_width=11)
#     date = Column(DateTime)
#     message = Column(LongText)
#     composed_rev = Column(TINYINT)
#     repository_id = Column(Integer, key=True, display_width=11)

#     def __repr__(self):
#         return "<Commit(id='%d', rev='%s', committer_id='%d', " + \
#             "author_id='%d', composed_rev='%s', repository_id='%s', " + \
#             "date='%s', message='%s')>" % \
#             (self.id, self.rev, self.committer_id,
#              self.author_id, self.composed_rev, self.repository_id,
#              self.date, self.message)


    
