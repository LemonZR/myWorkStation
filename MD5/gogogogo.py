#coding:utf8


import MD5
def lala(file,newfile):
    with open(newfile,'w') as nf:
        with open('%s' % (file), 'r') as ff:

            for line in ff.readlines():
                ls=line.split(",")
                ip=ls[0]
                pay_time=ls[1]
                pay_phone=ls[2]
                newpay_phone=MD5.md5_data(pay_phone)
                phone=ls[3]
                newphone=MD5.md5_data(phone)
                pay_order_id=ls[4]
                nf.write("%s,%s,%s,%s,%s"%(ip,pay_time,newpay_phone,newphone,pay_order_id))

if __name__=="__main__":
    lala("result.csv","new.csv")
