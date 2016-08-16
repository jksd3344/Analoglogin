#!/usr/bin/python
#coding=utf-8

"""
auth: suguoxin
mail: suguoxin@playcrab.com
createtime: 2016-02-17 11:20:00
usege: 发送邮件

"""
from __future__ import absolute_import
from custom.command import Custom_Command as cmd
import getopt
import sys


def send_email(param):

    from_who = param['from_who']
    if from_who is None:
        from_who = "data_dev"

    to_who = param['to_who']
    if to_who is None:
        print ("'The recipient' is not null")
        return False

    title = param['title']
    if title is None:
        print ("'Email title' is not null")
        return False

    content = param['content']
    if content is None:
        content = " "

    cmd_email = "curl -s --user 'api:key-5sah1hiwvoidgevxm5k6sjmfxe-l3y97' " \
                "https://api.mailgun.net/v2/playcrab.co/messages -F from='%s <%s@playcrab.com>' " \
                "-F to='%s' -F subject='%s' -F text='%s' -F o:tag='report' " \
                % (from_who, from_who, to_who, title, content)

    cc_who = param['cc_who']
    if cc_who is not None:
        cmd_email = "curl -s --user 'api:key-5sah1hiwvoidgevxm5k6sjmfxe-l3y97' " \
                    "https://api.mailgun.net/v2/playcrab.co/messages -F from='%s <%s@playcrab.com>' " \
                    "-F to='%s' -F cc='%s' -F subject='%s' -F text='%s' -F o:tag='report' " \
                    % (from_who, from_who, to_who, cc_who, title, content)

    attachment_file = param['attachment_file']

    if attachment_file is not None:

        cmd_email = "curl -s --user 'api:key-5sah1hiwvoidgevxm5k6sjmfxe-l3y97' " \
                    "https://api.mailgun.net/v2/playcrab.co/messages -F from='%s <%s@playcrab.com>' " \
                    "-F to='%s' -F cc='%s' -F subject='%s' -F text='%s' -F o:tag='report' " \
                    "-F attachment=@%s" % (from_who, from_who, to_who, cc_who, title, content, attachment_file)

    result = cmd.run(cmd_email)

    if result['status'] != 0:
        return False
    else:
        return True


def main(argv):
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hf:t:c:i:o:a:',
                                   ['help', 'from=', 'to=', 'cc=', 'title=', 'content=', 'attachment='])
    except getopt.GetoptError, err:
        print str(err)
        sys.exit(2)

    VARS = {'from_who': None, 'to_who': None, 'cc_who': None, 'title': None, 'content': None, 'attachment_file': None}

    for opt, value in opts:

        if opt in ('-h', '--help'):
            print "-f, --from 发件人. 只需写@之前的即可，如：suguoxin，如不写则默认：data_dev"
            print "-t, --to 收件人. 需要写全，多个则以逗号分隔，如：shoujianren_1@playcrab.com,shoujianren_2@playcrab.com"
            print "-c, --cc 抄送人. 需要写全，多个则以逗号分隔，如：shoujianren_1@playcrab.com,shoujianren_2@playcrab.com，可以为空"
            print "-i, --title 标题. 该封邮件的标题、题目，不允许为空"
            print "-o, --content 内容. 该封邮件的正文内容"
            print "-a, --attachment 附件. 该封邮件的附件，需要指明路径及文件名，如：/usr/loacl/test.txt，可以为空"
            sys.exit()

        if opt in ('-f', '--from'):
            VARS['from_who'] = value
        elif opt in ('-t', '--to'):
            VARS['to_who'] = value
        elif opt in ('-c', '--cc'):
            VARS['cc_who'] = value
        elif opt in ('-i', '--title'):
            VARS['title'] = value
        elif opt in ('-o', '--content'):
            VARS['content'] = value
        elif opt in ('-a', '--attachment'):
            VARS['attachment_file'] = value

    if send_email(VARS):
        print ("send email success")
    else:
        print ("send email error")

if __name__ == '__main__':
    main(sys.argv)



