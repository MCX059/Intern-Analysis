from bs4 import BeautifulSoup
import time
import pandas as pd
import pymysql
import numpy as np
from tqdm import tqdm
import urllib.request
import re
from sqlalchemy import create_engine


def log_err(id_: int, con: pymysql.Connection, err: str):
    """This function records the errors to the sql table.

    Arguments:
        id_ {int} -- current id
        con {pymysql.Connection} -- MySQL connection
        err {str} -- error type
    """
    sql = "INSERT INTO zhinanzhe.Err (idErr, reasons) VALUE (%s, '%s')" % (
        id_, err)
    cursor = con.cursor()
    cursor.execute(sql)
    cursor.close()
    con.commit()


def get_title(soup: BeautifulSoup) -> dict:
    """This function gets the title of the page.

    Arguments:
        soup {BeautifulSoup} -- BeautifulSoup Instance

    Returns:
        dict -- {'title': title}
    """
    title = soup.h1.text
    dic = {
        'title': title
    }
    return dic


def get_original(a: list, id_: int, con: pymysql.Connection) -> dict:
    """This function gets the original school, major and school level of one instance.

    Arguments:
        a {list} -- a list of words
        id_ {int} -- current id
        con {pymysql.Connection} -- MySQL connection

    Returns:
        dict -- {
            'original_school': original_school,
            'original_major': original_major,
            'school_level': school_level
        }
    """
    school_level = data['level'].iloc[id_]

    try:
        original_school = a[1].find_all(
            'div', attrs={'class': 'spani'})[0].text
    except:
        original_school = ''
        log_err(id_, con, 'MISSING ORIGINAL SCHOOL')

    try:
        original_major = a[1].find_all('div', attrs={'class': 'spani'})[1].text
    except:
        original_major = ''
        log_err(id_, con, 'MISSING ORIGINAL MAJOR')

    dic = {
        'original_school': original_school,
        'original_major': original_major,
        'school_level': school_level
    }
    return dic


def get_admitted(a: list, id_: int, con: pymysql.Connection) -> dict:
    """This function gets the admission information.

    Arguments:
        a {list} -- a list of words
        id_ {int} -- current id
        con {pymysql.Connection} -- MySQL connection

    Raises:
        ValueError: Null school name
        ValueError: Null major name

    Returns:
        dict -- {
            'admitted_school': admitted_school,
            'admitted_major': admitted_major,
            'location': location
        }
    """
    location = data['region'].iloc[id_]
    tmp = get_title(soup)
    tmp = tmp['title']

    try:
        admitted_school = a[2].find_all(
            'div', attrs={'class': 'spani'})[0].text
        if admitted_school == '':
            raise ValueError
    except:
        university_str = re.findall(r'(.*)(?<=大学)', tmp)
        if len(university_str) != 0:
            admitted_school = university_str[0]
        else:
            admitted_school = ''
            log_err(id_, con, 'MISSING ADMITTED SCHOOL')

    try:
        admitted_major = a[2].find_all('div', attrs={'class': 'spani'})[1].text
        if admitted_major == '':
            raise ValueError
    except:
        if re.search(r'分校', tmp) != None:
            try:
                admitted_major = re.findall(r'(?<=分校).*?(?=offer)', tmp)[0]
            except:
                admitted_major = ''
                log_err(id_, con, 'MISSING ADMITTED MAJOR')
        elif re.search(r'大学', tmp) != None:
            try:
                admitted_major = re.findall(r'(?<=大学).*?(?=offer)', tmp)[0]
            except:
                admitted_major = ''
                log_err(id_, con, 'MISSING ADMITTED MAJOR')
        elif re.search(r'学院', tmp) != None:
            try:
                admitted_major = re.findall(r'(?<=学院).*?(?=offer)', tmp)[0]
            except:
                admitted_major = ''
                log_err(id_, con, 'MISSING ADMITTED MAJOR')
        else:
            admitted_major = ''
            log_err(id_, con, 'MISSING ADMITTED MAJOR')

    dic = {
        'admitted_school': admitted_school,
        'admitted_major': admitted_major,
        'location': location
    }
    return dic


def get_hard(a: list, id_: int, con: pymysql.Connection) -> dict:
    """This function gets the hard background.

    Arguments:
        a {list} -- a list of words
        id_ {int} -- current id
        con {pymysql.Connection} -- MySQL connection

    Returns:
        dict -- {
            'GPA': gpa,
            'toefl': toefl,
            'ielts': ielts,
            'GRE': gre,
            'GMAT': gmat
        }
    """
    # OVERALL
    try:
        t = a[3].text.replace('\n', '')
    except:
        gpa = 'NULL'
        toefl = 'NULL'
        ielts = 'NULL'
        gre = 'NULL'
        gmat = 'NULL'
        log_err(id_, con, 'NOTHING HARD')
        dic = {
            'GPA': gpa,
            'toefl': toefl,
            'ielts': ielts,
            'GRE': gre,
            'GMAT': gmat
        }
        return dic

    # GPA
    try:
        gpa_str = re.findall(r'GPA.*\d+\.\d+|GPA.*\d+', t)[0]
        gpa = re.findall(r'\d+\.\d+|\d+', gpa_str)[0]
        gpa = float(gpa)
    except:
        gpa = 'NULL'
        log_err(id_, con, 'MISSING GPA')

    # Language
    toefl_str = re.findall(r'托福.*\d+', t)
    if len(toefl_str) == 0:
        toefl = 'NULL'
        try:
            ielts_str = re.findall(r'雅思.*\d\.\d+|雅思.*\d+', t)[0]
            ielts = re.findall(r'\d\.\d+|\d+', ielts_str)[0]
            ielts = float(ielts)
        except:
            ielts = 'NULL'
            log_err(id_, con, 'MISSING LANGUAGE')
    else:
        try:
            toefl_str = toefl_str[0]
            toefl = re.findall(r'\d+', toefl_str)[0]
            toefl = int(toefl)

            ielts_str = re.findall(r'雅思.*\d\.\d+|雅思.*\d+', t)
            if len(ielts_str) != 0:
                try:
                    ielts_str = ielts_str[0]
                    ielts = re.findall(r'\d\.\d+|\d+', ielts_str)[0]
                    ielts = float(ielts)
                except:
                    ielts = 'NULL'
            else:
                ielts = 'NULL'
        except:
            toefl = 'NULL'
            log_err(id_, con, 'MISSING LANGUAGE')

    # GRE
    try:
        gre_str = re.findall(r'GRE.*\d+', t)[0]
        gre = re.findall(r'\d+', gre_str)[0]
        gre = int(gre)
    except:
        gre = 'NULL'

    # GMAT
    try:
        gmat_str = re.findall(r'GMAT.*\d+', t)[0]
        gmat = re.findall(r'\d+', gmat_str)[0]
        gmat = int(gmat)
    except:
        gmat = 'NULL'

    dic = {
        'GPA': gpa,
        'toefl': toefl,
        'ielts': ielts,
        'GRE': gre,
        'GMAT': gmat
    }
    return dic


def get_background(a: list, id_: int, con: pymysql.Connection) -> (dict, list):
    """This function gets the soft background of the instance.

    Arguments:
        a {list} -- a list of words
        id_ {int} -- current id
        con {pymysql.Connection} -- MySQL connection

    Returns:
        dic -- {
            'exp_sum': exp_sum,
            'interns': interns,
            'competition': competition,
            'program': program,
            'oversea': oversea,
            'interns_bool': interns_bool,
            'competition_bool': competition_bool,
            'program_bool': program_bool,
            'oversea_bool': oversea_bool
        }
        bg_list -- a list contains the backgrounds
    """
    try:
        bg = a[4].find_all('div')[1].text.replace('\n', '').replace(' ', '')
    except:
        exp_sum = 0
        interns = 0
        competition = 0
        program = 0
        oversea = 0
        interns_bool = False
        competition_bool = False
        program_bool = False
        oversea_bool = False
        bg_list = []
        log_err(id_, con, 'MISSING BACKGROUND')
        dic = {
            'exp_sum': exp_sum,
            'interns': interns,
            'competition': competition,
            'program': program,
            'oversea': oversea,
            'interns_bool': interns_bool,
            'competition_bool': competition_bool,
            'program_bool': program_bool,
            'oversea_bool': oversea_bool
        }
        return dic, bg_list

    # split experiences
    bg = a[4].find_all('div')[1].text.replace(
        '\n', '').replace(' ', '').replace('\t', '').replace('\'', '')
    bg_list = re.split(r'\d+\.', bg)
    if '' in bg_list:
        bg_list.remove('')

    # exp_sum
    exp_sum = len(bg_list)

    # interns
    interns_list = [x for x in bg_list if re.search(r'实习|公司|助理', x) != None]
    interns = len(interns_list)

    # competition
    competition_list = [
        x for x in bg_list if re.search(r'比赛|竞赛|大赛', x) != None]
    competition = len(competition_list)

    # oversea
    oversea_list = [x for x in bg_list if re.search(r'交换|暑研', x) != None]
    oversea = len(oversea_list)

    # program
    program = exp_sum - interns - competition - oversea

    # bool
    interns_bool = interns != 0
    competition_bool = competition != 0
    oversea_bool = oversea != 0
    program_bool = program != 0

    dic = {
        'exp_sum': exp_sum,
        'interns': interns,
        'competition': competition,
        'program': program,
        'oversea': oversea,
        'interns_bool': interns_bool,
        'competition_bool': competition_bool,
        'program_bool': program_bool,
        'oversea_bool': oversea_bool
    }
    return dic, bg_list


def to_database(s: BeautifulSoup, a: list, id_: int, con: pymysql.Connection) -> int:
    """This function operates all the functions above and record the results into the MySQL database.

    Arguments:
        s {BeautifulSoup} -- BeautifulSoup Instance
        a {list} -- a list of words
        id_ {int} -- current id
        con {pymysql.Connection} -- MySQL connection

    Returns:
        int -- the sum of columns
    """
    dic_title = get_title(s)
    dic_original = get_original(a, id_, con)
    dic_admitted = get_admitted(a, id_, con)
    dic_hard = get_hard(a, id_, con)
    dic_bg, bg_list = get_background(a, id_, con)
    realm = data['realm'].iloc[id_]
    cursor = con.cursor()
    ins = {
        'id': id_
    }
    ins.update(dic_title)
    ins.update(dic_original)
    ins.update(dic_admitted)
    ins['realm'] = realm
    ins.update(dic_hard)
    ins.update(dic_bg)

    ins_keys = [x for x in ins.keys()]

    # BasicInformation
    for bi in range(len(ins)):
        if bi == 0:
            sql_basicinformation = "INSERT INTO zhinanzhe.BasicInformation (%s) VALUE (%s)" % (
                ins_keys[bi], ins[ins_keys[bi]])
        else:
            if type(ins[ins_keys[bi]]) == str and ins[ins_keys[bi]] != 'NULL':
                sql_basicinformation = "UPDATE zhinanzhe.BasicInformation SET %s = '%s' WHERE id = %s" % (
                    ins_keys[bi], ins[ins_keys[bi]], ins[ins_keys[0]])
            else:
                sql_basicinformation = "UPDATE zhinanzhe.BasicInformation SET %s = %s WHERE id = %s" % (
                    ins_keys[bi], ins[ins_keys[bi]], ins[ins_keys[0]])
        cursor.execute(sql_basicinformation)
        con.commit()

    # Admission
    sql_admission = "INSERT INTO zhinanzhe.Admission (id, admitted_major, admitted_school, location) \
                     VALUES (%s, '%s', '%s', '%s')" \
                     % (id_, dic_admitted['admitted_major'], dic_admitted['admitted_school'], dic_admitted['location'])
    cursor.execute(sql_admission)
    con.commit()

    # Background
    sql_bg_create = "INSERT INTO zhinanzhe.Background (id) VALUE (%s)" % (id_)
    cursor.execute(sql_bg_create)
    con.commit()
    if len(bg_list) == 0:
        sql_bg = "UPDATE zhinanzhe.Background SET %s = '%s' WHERE id = %s" % (
            'bg1', 'NO BACKGROUND', id_)
        cursor.execute(sql_bg)
        con.commit()
    elif len(bg_list) <= 15:
        for bg in range(len(bg_list)):
            sql_bg = "UPDATE zhinanzhe.Background SET %s = '%s' WHERE id = %s" % (
                'bg' + str(bg + 1), bg_list[bg], id_)
            cursor.execute(sql_bg)
            con.commit()
    else:
        for bg in range(15):
            sql_bg = "UPDATE zhinanzhe.Background SET %s = '%s' WHERE id = %s" % (
                'bg' + str(bg + 1), bg_list[bg], id_)
            cursor.execute(sql_bg)
            con.commit()
    con.close()
    return len(ins)


if __name__ == '__main__':
    start = int(input('Start from:\n'))
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'
    }
    # 读数据库并保存到df中
    connection = create_engine(
        'mysql+pymysql://root:epaiiplus1E0@127.0.0.1:3306/zhinanzhe')
    data = pd.read_sql_table('Pre', connection, index_col='id')

    for i in tqdm(range(start, len(data))):
        url = 'https:' + data['url'].iloc[i]
        request = urllib.request.Request(url=url, headers=headers)
        response = urllib.request.urlopen(request).read()
        html = response.decode()
        soup = BeautifulSoup(html, 'lxml')
        useful = soup.find_all('div', attrs={'class': 'd-line'})
        if len(useful) == 0:
            print('Oops! ' + str(i) + ' ERROR!')
            log_err(i, con, 'MISSING EVERYTHING')
        else:
            con = pymysql.connect(host='localhost', port=3306,
                                  user='root', password='epaiiplus1E0', db='zhinanzhe')
            to_database(soup, useful, i, con)
    print('Done!')
