
thisscript="$(basename "$(test -L "$0" && readlink "$0" || echo "$0")")"
hadoopconf_dir=${PWD}/conf/
log_file=${hadoopconf_dir}/../logs/hadoop_enabler_RACKAWARENESS.log
function log {
	echo "$(date) [${thisscript}] $1" >> ${log_file}
}

log ">>>>>>>>> Request Set received.  Begining processing >>>>>>>>>" 
log "$# addresses in current Request Set"
log "Config directory is [${hadoopconf_dir}]"

env_file=${PWD}/bin/hadoop_enabler_ENVIRONMENT.sh
if [ -f ${env_file} ]; then
	. ${env_file}
else
	log "ERROR - Unable to find environment script [${env_file}].  All Nodes will be assigned to default rack."
	hadoop_enabler_RACKAWARENESS_POLICY=DEFAULT   
fi

if [ -z ${hadoop_enabler_RACKAWARENESS_DEFAULT} ]; then
	hadoop_enabler_RACKAWARENESS_DEFAULT=/default/rack
	log "WARNING - Required variable [hadoop_enabler_RACKAWARENESS_DEFAULT] is not set.  Will use the value [${hadoop_enabler_RACKAWARENESS_DEFAULT}] as the default rack."
else
	log "Default rack is ${hadoop_enabler_RACKAWARENESS_DEFAULT}"
fi

log "Rackawareness policy is [$hadoop_enabler_RACKAWARENESS_POLICY]"
case $hadoop_enabler_RACKAWARENESS_POLICY in
	CONFIG_FILE)
		if [ -z ${hadoop_enabler_RACKAWARENESS_CONFIG_FILE} ] ; then
			log "WARNING - Required variable [hadoop_enabler_RACKAWARENESS_CONFIG_FILE] is not set.  Will use DEFAULT policy instead."
			hadoop_enabler_RACKAWARENESS_POLICY=DEFAULT
		else
			log "Looking for Topology Configuration Table in [${hadoop_enabler_RACKAWARENESS_CONFIG_FILE}]"

			if [ -f ${hadoop_enabler_RACKAWARENESS_CONFIG_FILE} ]; then
				log "----- Topology Configuration File - [Start] -----"
				cat ${hadoop_enabler_RACKAWARENESS_CONFIG_FILE} >> ${log_file}
				log "----- Topology Configuration File - End  -----"
			else 
				log "WARNING - Topology Configuration file does not exist.  Will use DEFAULT policy instead."
				hadoop_enabler_RACKAWARENESS_POLICY=DEFAULT
			fi
		fi 
		;;
	ENV_VARIABLE)
		if [ -z ${hadoop_enabler_RACKAWARENESS_ENV_VARIABLE} ] ; then
			log "WARNING - Required variable [hadoop_enabler_RACKAWARENESS_ENV_VARIABLE] is not set.  Will use DEFAULT policy instead."
			hadoop_enabler_RACKAWARENESS_POLICY=DEFAULT
		else
			log "Will use Environment Variable [${hadoop_enabler_RACKAWARENESS_ENV_VARIABLE}] on remote nodes to set Rack value"
		fi 
		;;
	DEFAULT)
		log "The DEFAULT policy has been selected."
		;;
	*)
		log "WARNING - Rackawareness Policy  is not recognized. Will use DEFAULT policy."

esac

while [ $# -gt 0 ] ; do

	nodeArg=$1
	log  "Processing request for Node [${nodeArg}]"

	result="" 

	case ${hadoop_enabler_RACKAWARENESS_POLICY} in
		CONFIG_FILE)
			exec< ${hadoop_enabler_RACKAWARENESS_CONFIG_FILE} 

			while read line ; do
			    	# Match parameter to line
				arg=( $line ) 
				if [ ${arg[0]:0:1} != "#" ]; then
					case $nodeArg in
					     ${arg[0]} )
						 result="${arg[1]}"
						 ;;
					    * ) 
						 ;;
					esac
				fi
			done 
			if [ -z ${result} ] ; then
				log "WARNING - No match found for current node in config file.  Will use DEFAULT policy instead for the current node."
			fi
			;;
		ENV_VARIABLE)
			testvar=`( ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null $nodeArg set | grep ${hadoop_enabler_RACKAWARENESS_ENV_VARIABLE}= ) 2> out.log`
			ret=$?		
			case $ret in
				0)
					#echo $testvar
					#echo `expr index $testvar "="`
					equalpos=`expr index $testvar "="`
					#echo $equalpos
					result=${testvar:${equalpos}}
					;;
				1)
					log "WARNING - Environment variable [${hadoop_enabler_RACKAWARENESS_ENV_VARIABLE}] not set on remote host.  Will use DEFAULT policy instead for the current node."
					;;
				*)
					cat out.log >> ${log_file}
					log "WARNING - Unknown returncode [$ret] while trying to retrieve environment variable from host.  Will use DEFAULT policy instead for the current node."
			esac
	esac

	#DEFAULT Policy	
	if [ -z ${result} ] ; then
	  	result="${hadoop_enabler_RACKAWARENESS_DEFAULT} "
	fi

	log "Rack for [${nodeArg}] is [$result]"
	echo -n "$result "

	shift
 
done 

log "<<<<<<<<< Request Set completed.  Exiting processing <<<<<<<<<"

