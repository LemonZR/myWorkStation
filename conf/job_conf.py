#coding:utf8
'''
        year (int|str) – 4-digit year
        month (int|str) – month (1-12)
        day (int|str) – day of the (1-31)
        week (int|str) – ISO week (1-53)
        day_of_week (int|str) – number or name of weekday (0-6 or mon,tue,wed,thu,fri,sat,sun)
        hour (int|str) – hour (0-23)
        minute (int|str) – minute (0-59)
        second (int|str) – second (0-59)

        start_date (datetime|str) – earliest possible date/time to trigger on (inclusive)
        end_date (datetime|str) – latest possible date/time to trigger on (inclusive)
        timezone (datetime.tzinfo|str) – time zone to use for the date/time calculations (defaults to scheduler timezone)

        *    any    Fire on every value
        */a    any    Fire every a values, starting from the minimum
        a-b    any    Fire on any value within the a-b range (a must be smaller than b)
        a-b/c    any    Fire every c values within the a-b range
        xth y    day    Fire on the x -th occurrence of weekday y within the month
        last x    day    Fire on the last occurrence of weekday x within the month
        last    day    Fire on the last day within the month
        x,y,z    any    Fire on any matching expression; can combine any number of any of the above expressions
    '''
#数据表格文件名称  注：此处的key与query_SQL.py文件中的sql的key对应
file_name = {
		'shibai':'周失败',
		'wed_hd':'周三活动',
        'zhoucf':'一期周失败重发',
        'sbcf2':'二期周失败重发',
        'tucao':'吐槽数据',
        'jkfwtj':'接口访问统计',
	    'yueshibai':'月充值失败',
        'yuesbcf':'一期月失败重发',
        'ysbcf2':'二期月失败重发',
		'wed_hd_xin':'周三活动新'
        }

#基准日期 0为当天
date_num = 3
#date_range=20170929-201709
#数据库名称／SQL／excel文件名／执行时间(年/月/日/时/分/秒/星期(0-6))
#eg:ecora|shibai|周失败|2017 09 22 10 50 0 4
conf_list=[
    "ecora|zhoucf|一期周失败重发|* * * 11 5 0 4",
    "ecora2|sbcf2|二期周失败重发|* * * 17 48 0 0",
    "ecora|shibai|周失败|* * * 11 7 0 4",
    "ecora2|ysbcf2|二期月失败重发|* * * 15 44 0 0",
    "ecora|yuesbcf|一期月失败重发|* * * 15 26 0 0",
    "zxkf|pingjiashujv|评价数据|* * * 13 41 0 2",
    "kqzx_test|pingjiashujv|对账|* * * 13 55 0 2",
]