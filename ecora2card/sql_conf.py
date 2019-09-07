#coding:utf8

sybase_sql = '''
select
'%s' as PcardId,
f.operate_id as ActId,
v.channelID as channelID,
f.prov_code as provCode,
f.sale_type as saleType,
case l.limit_type  when 1 then l.limit_param else 0 end as targetNoType,
f.begin_time as beginTime,
f.end_time as endTime,
f.sale_code as saleCode,
str_replace(f.sale_desc,char(10),'') as saleDesc,
f.present_name as presentName,
f.discount_format as discountFormat,
getdate() as createTime,
getdate() as opTime,
'15032609451' as createUser,
'15032609451' as opUser

from td_padm_pay_fav_info f
left join
(select u.key_id as keyid, case u.pc when null then u.pc else u.pc+'|' end + case u.app when null then u.app else u.app+'|' end +case u.els when null then u.els else u.els +'|' end as channelID
from
(select c.key_id ,
max(case c.pub_channel when '00' then c.pub_channel else null end) as pc,
max(case c.pub_channel when '11' then c.pub_channel else null end ) as app,
max(case c.pub_channel when '12' then c.pub_channel else null end )as els
from TD_PADM_PAY_CHANNEL_INFO c group by c.key_id ) u )v on f.operate_id = v.keyid
left join td_ptl_favinfo_limit l on l.key_id = f.key_id
where f.activity_type ='2' 
and  f.operate_id =%s
'''


inser_sql ="insert into echnmarketdb2.td_pcard_charge_act_info values(%s)"
pcard_act_sql = "select PCARD_ID,BUSI_ACT_ID   from echnmarketdb2.td_def_pcard_busi_rel where BUSI_TYPE='hf01';"

sybase_sql_test = "select %s,name,null  from zhangruitest.insert_test where name =%s"
