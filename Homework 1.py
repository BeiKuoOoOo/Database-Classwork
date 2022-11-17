from sqlalchemy import desc, distinct
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func
from sqlalchemy import create_engine

engine = create_engine(
      "mysql+pymysql://root:password!@localhost/sys", echo=True)
conn = engine.connect()


from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Integer, String, Column, DateTime
Base = declarative_base()

class Sailor(Base):
    __tablename__ = 'sailors'

    sid = Column(Integer, primary_key=True)
    sname = Column(String)
    rating = Column(Integer)
    age = Column(Integer)

    def __repr__(self):
        return "<Sailor(id=%s, name='%s', rating=%s)>" % (self.sid, self.sname, self.age)

from sqlalchemy import ForeignKey
from sqlalchemy.orm import backref, relationship

class Boat(Base):
    __tablename__ = 'boats'

    bid = Column(Integer, primary_key=True)
    bname = Column(String)
    color = Column(String)
    length = Column(Integer)

    reservations = relationship('Reservation',
                                backref=backref('boat', cascade='delete'))

    def __repr__(self):
        return "<Boat(id=%s, name='%s', color=%s)>" % (self.bid, self.bname, self.color)

from sqlalchemy import PrimaryKeyConstraint

class Reservation(Base):
    __tablename__ = 'reserves'
    __table_args__ = (PrimaryKeyConstraint('sid', 'bid', 'day'), {})

    sid = Column(Integer, ForeignKey('sailors.sid'))
    bid = Column(Integer, ForeignKey('boats.bid'))
    day = Column(DateTime)

    sailor = relationship('Sailor')

    def __repr__(self):
        return "<Reservation(sid=%s, bid=%s, day=%s)>" % (self.sid, self.bid, self.day)

class Employee(Base):
    __tablename__ = 'employees'

    eid = Column(Integer, primary_key=True)
    ename = Column(String)
    hourlyWage = Column(Integer)
    jobType = Column(String)

    def __repr__(self):
        return "<Employee(eid=%s, ename='%s', hourlyWage=%s)>" % (self.eid, self.ename, self.hourlyWage)

class WeeklySchedule(Base):
    __tablename__ = 'weeklySchedule'

    logid = Column(Integer, primary_key=True)
    eid = Column(Integer)
    hourPerWeek = Column(Integer)
    weekStartDay = Column(DateTime)
    overtime = Column(Integer)

    def __repr__(self):
        return "<WeeklySchedule(logid=%s, eid=%s, hourPerWeek=%s)>" % (self.logid, self.eid, self.hourPerWeek)


Base.metadata.create_all(engine)

conn = engine.connect()

Session = sessionmaker(bind=engine)

# ----------Q2-----------

session = Session()

def q1():
    sql_query = conn.execute("select boats.bid, boats.bname, count(*) from boats, reserves where boats.bid = reserves.bid group by boats.bid;").fetchall()
    orm_query = session.query(Boat.bid, Boat.bname, func.count(Reservation.bid)).join(Reservation).group_by(Boat.bid).all()
    assert sql_query == orm_query

def q2():
    sql_query = conn.execute("select s.sid, s.sname from sailors s where not exists( select bid from boats b where b.color = 'red' "
                             "and not exists(select r.bid from reserves r where r.bid = b.bid and r.sid = s.sid));").fetchall()
    filter_red = session.query(Boat.bid).filter(Boat.color == 'red')
    count_red = filter_red.count()
    orm_query = session.query(Sailor.sid, Sailor.sname).filter(Reservation.bid.in_(filter_red)).join(Reservation).group_by(Reservation.sid).having(func.count(distinct(Reservation.bid)) == count_red).all()
    assert sql_query == orm_query

def q3():
    sql_query = conn.execute("select distinct s.sid, s.sname from sailors s, reserves r, boats b where b.color = 'red' and s.sid = r.sid and r.bid = b.bid and s.sid not in (select s.sid from sailors s, reserves r, boats b where s.sid = r.sid and b.color !='red' and r.bid = b.bid);").fetchall()
    filter_red = session.query(Boat.bid).filter(Boat.color == 'red')
    filter_notRed = session.query(Boat.bid).filter(Boat.color != 'red')
    query_red = session.query(Reservation.sid).filter(Reservation.bid.in_(filter_red))
    query_notRed = session.query(Reservation.sid).filter(Reservation.bid.in_(filter_notRed))
    orm_query = session.query(Sailor.sid, Sailor.sname).filter(Sailor.sid.in_(query_red)).filter(Sailor.sid.notin_(query_notRed)).all()
    assert sql_query == orm_query

def q4():
    sql_query = conn.execute("select r.bid, b.bname, count(r.bid) value_occurrence from reserves r, boats b where r.bid = b.bid group by r.bid order by value_occurrence desc limit 1;").fetchall()
    orm_query = session.query(Reservation.bid, Boat.bname,func.count(Reservation.bid)).filter(Reservation.bid == Boat.bid).group_by(Reservation.bid).order_by(desc(func.count(Reservation.bid))).limit(1).all()
    assert sql_query == orm_query

def q5():
    sql_query = conn.execute("select s.sname from sailors s where s.sid not in (select r.sid from reserves r inner join boats b on b.bid = r.bid where b.color = 'red');").fetchall()
    filter_red = session.query(Reservation.sid).join(Boat).filter(Boat.bid == Reservation.bid).filter(Boat.color == 'red')
    orm_query = session.query(Sailor.sname).filter(Sailor.sid.notin_(filter_red)).all()
    assert sql_query == orm_query

def q6():
    sql_query = conn.execute("select avg(s.age) from sailors s where s.rating = 10;").fetchall()
    orm_query = session.query(func.avg(Sailor.age)).filter(Sailor.rating == 10).all()
    assert sql_query == orm_query

def q7():
    sql_query = conn.execute("select s.rating, s.sname, s.sid, s.age from sailors s inner join (select s1.rating, min(s1.age) as minAge from sailors s1 group by s1.rating) s1 on s.rating = s1.rating and s1.minAge = s.age order by s.rating;").fetchall()
    s1 = session.query(Sailor.rating, func.min(Sailor.age).label('minAge')).group_by(Sailor.rating).subquery()
    orm_query = session.query(Sailor.rating, Sailor.sname, Sailor.sid, Sailor.age).filter(Sailor.age == s1.c.minAge, Sailor.rating == s1.c.rating).order_by(Sailor.rating).all()
    assert sql_query == orm_query

def q8():
    sql_query = conn.execute("select distinct b.bname, s.sname, b.bid, count(*) from boats b "
                             "inner join reserves r on b.bid = r.bid "
                             "inner join sailors s on r.sid = s.sid "
                             "group by b.bid, s.sid having count(*) >= all ("
                             "select count(*) from reserves r1 where r1.bid = b.bid group by r1.sid);").fetchall()
    countRes = session.query(Reservation.sid, Reservation.bid, Boat.bname, func.count(Reservation.bid).label('resNum')).filter(Reservation.bid == Boat.bid).group_by(Reservation.sid, Boat.bid).subquery()
    maxCount = session.query(countRes.c.bid, func.max(countRes.c.resNum).label('maxNum')).group_by(countRes.c.bid).subquery()
    orm_query = session.query(countRes.c.bname, Sailor.sname, countRes.c.bid, countRes.c.resNum).filter(countRes.c.bid == maxCount.c.bid).filter(countRes.c.resNum == maxCount.c.maxNum).filter(Sailor.sid == countRes.c.sid).order_by(countRes.c.bname, Sailor.sname).all()
    assert sql_query == orm_query


q1()
q2()
q3()
q4()
q5()
q6()
q7()
q8()

# ----------Q3-----------

def t1():
    sql_query = conn.execute("select w.eid, e.ename, e.hourlyWage*w.hourPerWeek + w.overtime*e.hourlyWage as totalPayment from employees e, weeklyschedule w where w.weekStartDay = '2022-10-03' and w.eid = e.eid;").fetchall()
    orm_query = session.query(WeeklySchedule.eid, Employee.ename, (Employee.hourlyWage*WeeklySchedule.hourPerWeek + WeeklySchedule.overtime*Employee.hourlyWage)).filter(WeeklySchedule.weekStartDay == "2022-10-03").filter(WeeklySchedule.eid == Employee.eid).all()
    assert sql_query == orm_query

def t2():
    sql_query = conn.execute("select w.eid, e.ename, max(w.hourPerWeek + w.overtime) from employees e, weeklyschedule w where w.eid = e.eid and w.weekStartDay = '2022-09-26'").fetchall()
    orm_query = session.query(WeeklySchedule.eid, Employee.ename, func.max(WeeklySchedule.hourPerWeek + WeeklySchedule.overtime)).filter(WeeklySchedule.eid == Employee.eid).filter(WeeklySchedule.weekStartDay == '2022-09-26').all()
    assert  sql_query == orm_query

t1()
t2()