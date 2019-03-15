from datetime import datetime, timedelta
from flask import (
    Flask, request, render_template, Blueprint
)
import re, os, traceback, cx_Oracle
from loguru import logger


app = Flask(__name__)
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
    except Exception as e:
        traceback.print_exc()
        con.rollback()
    finally:
        cursor.close()
        con.close()
    return result


def removeBlank(str):
    if str is None:
        return None

    pattern = re.compile(r'[\\s]')
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

    if int(beginDate) > int(endDate):
        flash('开始日期不能大于结束日期!')
        return render_template('invt_statistics.html', invtIn=[], invtOut=[], beginDate=beginTime.strftime('%Y-%m-%d'), endDate=now.strftime('%Y-%m-%d'))

    if int(endDate) - int(beginDate) > 30:
        flash('不能统计大于一个月的数据!')
        return render_template('invt_statistics.html', invtIn=[], invtOut=[], beginDate=beginTime.strftime('%Y-%m-%d'), endDate=now.strftime('%Y-%m-%d'))

    sql = '''
    select decode(grouping(to_char(t.sys_date, 'yyyy-MM-dd')), 1, '小计', to_char(t.sys_date, 'yyyy-MM-dd')), count(1) day_all
from ceb2_invt_head t
where t.sys_date >= to_date(:beginDate, 'yyyyMMddHH24')
and t.sys_date < to_date(:endDate, 'yyyyMMdd')
and t.app_status in('399', '800', '899')
group by rollup(to_char(t.sys_date, 'yyyy-MM-dd'))
order by to_char(t.sys_date, 'yyyy-MM-dd'), day_all desc
    '''
    try:
        logger.info('beginDate={}, oldBeginDate={}', beginDate, oldBeginDate)
        logger.info('endDate={}, oldEndDate={}', endDate, oldEndDate)
        beginTime = datetime.strptime(beginDate, '%Y%m%d')
        endTime = datetime.strptime(endDate, '%Y%m%d')
    except:
        return render_template('invt_statistics.html', invtIn=[], invtOut=[], beginDate=beginTime.strftime('%Y-%m-%d'), endDate=now.strftime('%Y-%m-%d'), message='日期格式不对!')

    invtInResult = executeSql(sql, beginDate=beginDate + '00', endDate=endDate)
    invtOutResult = executeSql(sql.replace('ceb2', 'ceb3'), beginDate=beginDate + '00', endDate=endDate)
    return render_template('invt_statistics.html', invtIn=invtInResult, invtOut=invtOutResult, beginDate=beginTime.strftime('%Y-%m-%d'), endDate=endTime.strftime('%Y-%m-%d'))


app.register_blueprint(bp)
