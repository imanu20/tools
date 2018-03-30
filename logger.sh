#! /bin/bash
############################################
#
# Author:
# Create time: 2017 12月 09 16时50分34秒
# E-Mail: xiaojian.jia@163.com
# version 1.1
#
############################################
#!/bin/bash
#可将log函数单独放一个文件，通过.命令引入，这样就可以共用了
#. log.sh 
#设置日志级别
loglevel=0 #debug:0; info:1; warn:2; error:3
logfile=$0".log"
function log {
    local msg;
    local logtype;
    local sep=" ";
    logtype=$1
    msg=${@:2}
    datetime=`date +'%F %H:%M:%S'`
    logformat="[${logtype}]${sep}${datetime}${sep}[`caller 1|awk '{print $3}'`:`caller 1 | awk '{print $1}'`]${sep}${msg}"
    {
        case $logtype in 
            debug)
                [[ $loglevel -le 0 ]] && echo -e "\033[30m${logformat}\033[0m" ;;
            info)
                [[ $loglevel -le 1 ]] && echo -e "\033[32m${logformat}\033[0m" ;;
            warn)
                [[ $loglevel -le 2 ]] && echo -e "\033[33m${logformat}\033[0m" ;;
            error)
                [[ $loglevel -le 3 ]] && echo -e "\033[31m${logformat}\033[0m" ;;
        esac
    } | tee -a $logfile
}

LOGDEBUG() {
    #log debug "there are $# parameters:$@"
    log debug $@
}
LOGINFO() {
    #log info "funcname:${FUNCNAME[@]},lineno:$LINENO"
    log info $@
}
LOGWARN() {
    #log warn "funcname:${FUNCNAME[0]},lineno:$LINENO"
    log warn $@
}
LOGERROR() {
    #log error "the first para:$1;the second para:$2"
    log error $@
}
#set -x
LOGDEBUG first second
#set +x
LOGINFO first second
LOGWARN first second 
LOGERROR first second
