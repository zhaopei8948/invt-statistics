from datetime import datetime, timedelta
from flask import (
    Flask, request, render_template, Blueprint
)
import re, os, traceback, cx_Oracle
from loguru import logger


app = Flask(__name__)
app.config['SECRET_KEY'] = 'maintain-invtStatistics'

bp = Blueprint('invtStatistics', __name__, url_prefix='/maintain/statistics', static_folder='static')
username = os.getenv('ORCL_USERNAME') or 'username'
password = os.getenv('ORCL_PASSWORD') or 'password'
dbUrl = os.getenv('ORCL_DBURL') or '127.0.0.1:1521/orcl'


def executeSql(sql, **kw):
    logger.info("sql={}", sql)
    for k in kw:
        logger.info("kw ==k={}, v={}", k, kw[k])
    con = cx_Oracle.connect(username, password, dbUrl)
    cursor = con.cursor()
    result = None
    try:
        cursor.prepare(sql)
        cursor.execute(None, kw)
        result = cursor.fetchall()
    except Exception:
        traceback.print_exc()
        con.rollback()
    finally:
        cursor.close()
        con.close()
    return result

def executeSqlOne(sql, **kw):
    logger.info("sql={}", sql)
    for k in kw:
        logger.info("kw ==k={}, v={}", k, kw[k])
    con = cx_Oracle.connect(username, password, dbUrl)
    cursor = con.cursor()
    result = None
    try:
        cursor.prepare(sql)
        cursor.execute(None, kw)
        result = cursor.fetchone()
    except Exception:
        traceback.print_exc()
        con.rollback()
    finally:
        cursor.close()
        con.close()
    return result


def removeBlank(str):
    if str is None:
        return None

    pattern = re.compile(r'[\\s\'\"\\\\/]')
    return re.sub(pattern, '', str)


@bp.route("/invtStatisticsByDate")
def invtStatisticsByDate():
    oldBeginDate = request.values.get('beginDate')
    oldEndDate = request.values.get('endDate')

    now = datetime.now()
    beginTime = now + timedelta(days=-7)

    if oldBeginDate is None or oldEndDate is None:
        return render_template('invt_statistics.html', invtIn=[], invtOut=[], beginDate=beginTime.strftime('%Y-%m-%d'), endDate=now.strftime('%Y-%m-%d'))

    if oldBeginDate is not None:
        oldBeginDate = removeBlank(oldBeginDate)
        beginDate = oldBeginDate.replace('-', '')
    if oldEndDate is not None:
        oldEndDate = removeBlank(oldEndDate)
        endDate = oldEndDate.replace('-', '')

    try:
        logger.info('beginDate={}, oldBeginDate={}', beginDate, oldBeginDate)
        logger.info('endDate={}, oldEndDate={}', endDate, oldEndDate)
        beginTime = datetime.strptime(beginDate, '%Y%m%d')
        endTime = datetime.strptime(endDate, '%Y%m%d')
    except:
        return render_template('invt_statistics.html', invtIn=[], invtOut=[], beginDate=beginTime.strftime('%Y-%m-%d'), endDate=now.strftime('%Y-%m-%d'), message='日期格式不对!')

    if int(beginDate) > int(endDate):
        msg = '开始日期不能大于结束日期!'
        return render_template('invt_statistics.html', invtIn=[], invtOut=[], beginDate=beginTime.strftime('%Y-%m-%d'), endDate=now.strftime('%Y-%m-%d'), message=msg)

    logger.info('endTime - beginTime = {} days.', (endTime - beginTime).days)
    if (endTime - beginTime).days > 30:
        msg = '不能统计大于一个月的数据!'
        return render_template('invt_statistics.html', invtIn=[], invtOut=[], beginDate=beginTime.strftime('%Y-%m-%d'), endDate=now.strftime('%Y-%m-%d'), message=msg)

    sql = '''
    select decode(grouping(to_char(t.sys_date, 'yyyy-MM-dd')), 1, '小计',
    to_char(t.sys_date, 'yyyy-MM-dd')), count(1) day_all, sum(t0.total_price) from ceb2_invt_head t
    inner join (
        select tt.head_guid, sum(tt1.total_price * tt2.rmb_rate) total_price from ceb2_invt_head tt
        inner join ceb2_invt_list tt1 on tt1.head_guid = tt.head_guid
        left outer join exchrate tt2 on tt2.curr_code = tt1.currency
        inner join (
              select e.curr_code, max(e.begin_date) max_date from exchrate e group by e.curr_code
        ) tt3 on tt3.curr_code = tt2.curr_code
        and tt2.begin_date = tt3.max_date
        where tt.sys_date >= to_date(:innerBeginDate, 'yyyyMMdd')
        and tt.sys_date < to_date(:innerEndDate, 'yyyyMMdd')
        and tt.app_status in ('800', '899')
        group by tt.head_guid
    ) t0 on t0.head_guid = t.head_guid
    where t.sys_date >= to_date(:beginDate, 'yyyyMMdd')
    and t.sys_date < to_date(:endDate, 'yyyyMMdd')
    and t.app_status in ('800', '899')
    group by rollup(to_char(t.sys_date, 'yyyy-MM-dd'))
    order by to_char(t.sys_date, 'yyyy-MM-dd'), day_all desc
    '''

    invtInResult = executeSql(sql, beginDate=beginDate, endDate=endDate, innerBeginDate=beginDate, innerEndDate=endDate)
    invtOutResult = executeSql(sql.replace('ceb2', 'ceb3'), beginDate=beginDate, endDate=endDate, innerBeginDate=beginDate, innerEndDate=endDate)
    return render_template('invt_statistics.html', invtIn=invtInResult, invtOut=invtOutResult, beginDate=beginTime.strftime('%Y-%m-%d'), endDate=endTime.strftime('%Y-%m-%d'))

@bp.route("/invtReleaseStatisticsByDate")
def invtReleaseStatisticsByDate():
    oldBeginDate = request.values.get('beginDate')
    oldEndDate = request.values.get('endDate')

    now = datetime.now()
    beginTime = now + timedelta(days=-7)

    if oldBeginDate is None or oldEndDate is None:
        return render_template('invt_statistics.html', invtIn=[], invtOut=[], beginDate=beginTime.strftime('%Y-%m-%d'), endDate=now.strftime('%Y-%m-%d'))

    if oldBeginDate is not None:
        oldBeginDate = removeBlank(oldBeginDate)
        beginDate = oldBeginDate.replace('-', '')
    if oldEndDate is not None:
        oldEndDate = removeBlank(oldEndDate)
        endDate = oldEndDate.replace('-', '')

    try:
        logger.info('beginDate={}, oldBeginDate={}', beginDate, oldBeginDate)
        logger.info('endDate={}, oldEndDate={}', endDate, oldEndDate)
        beginTime = datetime.strptime(beginDate, '%Y%m%d')
        endTime = datetime.strptime(endDate, '%Y%m%d')
    except:
        return render_template('invt_statistics.html', invtIn=[], invtOut=[], beginDate=beginTime.strftime('%Y-%m-%d'), endDate=now.strftime('%Y-%m-%d'), message='日期格式不对!')

    if int(beginDate) > int(endDate):
        msg = '开始日期不能大于结束日期!'
        return render_template('invt_statistics.html', invtIn=[], invtOut=[], beginDate=beginTime.strftime('%Y-%m-%d'), endDate=now.strftime('%Y-%m-%d'), message=msg)

    sql = '''
        select count(1) 总单量, sum((case when t.app_status = '800' then 1 else 0 end)) 外网放行单量,
        round(sum((case when t.app_status = '800' then 1 else 0 end)) / count(1), 4) * 100 || '%' 外网放行比例,
         sum((case when t.cus_status = '26' then 1 else 0 end)) 内网放行单量,
         round(sum((case when t.cus_status = '26' then 1 else 0 end)) / count(1), 4) * 100 || '%' 内网放行比例
         from ceb2_invt_head t
        where t.sys_date >= to_date(:beginDate, 'yyyyMMdd')
        and t.sys_date < to_date(:endDate, 'yyyyMMdd')
    '''
    invtInResult = executeSqlOne(sql, beginDate=beginDate, endDate=endDate)
    sql = '''
       select count(1) 总单量, sum((case when t.app_status in ('800', '899') then 1 else 0 end)) 外网放行单量,
        round(sum((case when t.app_status in ('800', '899') then 1 else 0 end)) / count(1), 4) * 100 || '%' 外网放行比例,
         sum((case when t.cus_status in('26', '21') then 1 else 0 end)) 内网放行单量,
         round(sum((case when t.cus_status in ('26', '21') then 1 else 0 end)) / count(1), 4) * 100 || '%' 内网放行比例
         from ceb3_invt_head t
        where t.sys_date >= to_date(:beginDate, 'yyyyMMdd')
        and t.sys_date < to_date(:endDate, 'yyyyMMdd')
    '''
    invtOutResult = executeSqlOne(sql, beginDate=beginDate, endDate=endDate)
    return render_template('invt_release_statistics.html', invtIn=invtInResult, invtOut=invtOutResult, beginDate=beginTime.strftime('%Y-%m-%d'), endDate=endTime.strftime('%Y-%m-%d'))

app.register_blueprint(bp)
