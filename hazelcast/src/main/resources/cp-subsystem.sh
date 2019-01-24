#!/bin/sh

if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
   	echo "Sends CP subsystem management operations to a Hazelcast instance."
   	echo "parameters: "
   	echo "	-o, --operation	            : Operations are 'get-local-cp-member' | 'get-cp-groups' || 'get-cp-group' || 'force-destroy-cp-group' || 'get-cp-members' || 'promote-to-cp-member' || 'remove-cp-member' || 'restart' || 'get-cp-sessions' || 'force-close-cp-session'."
    echo "	-c, --cp-group       	    : Name of the CP group. Must be provided for 'get-cp-group', 'force-destroy-cp-group', 'get-cp-sessions', 'force-close-cp-session'."
    echo "	-m, --cp-member      	    : UUID of the CP member. Must be provided for 'remove-cp-member'."
    echo "	-s, --cp-session-id 	    : CP Session ID. Must be provided for 'force-close-cp-session'."
    echo "	-a, --address  	            : Defines which ip address hazelcast is running. Default value is '127.0.0.1'."
   	echo "	-p, --port  	            : Defines which port hazelcast is running. Default value is '5701'."
   	echo "	-g, --groupname             : Defines groupname of the cluster. Default value is 'dev'."
   	echo "	-P, --password              : Defines password of the cluster. Default value is 'dev-pass'."
   	exit 0
fi

while [ $# -gt 1 ]
do
key="$1"
case "$key" in
  	-o|--operation)
    OPERATION="$2"
    shift # past argument
    ;;
    -c|--cp-group)
    CP_GROUP_NAME="$2"
    shift # past argument
    ;;
    -m|--cp-member)
    CP_MEMBER_UID="$2"
    shift # past argument
    ;;
    -s|--cp-session-id)
    CP_SESSION_ID="$2"
    shift # past argument
    ;;
    -p|--port)
    PORT="$2"
    shift # past argument
    ;;
    -g|--groupname)
    GROUPNAME="$2"
    shift # past argument
    ;;
    -P|--password)
    PASSWORD="$2"
    shift # past argument
    ;;
     -a|--address)
    ADDRESS="$2"
    shift # past argument
    ;;
    *)
esac
shift # past argument or value
done


if [ -z "$OPERATION" ]; then
 	echo "No operation is defined, running script with default operation: 'get-local-cp-member'."
 	OPERATION="get-local-cp-member"
fi


if [[ -z "$PORT" ]]; then
    echo "No port is defined, running script with default port: '5701'."
    PORT="5701"
fi

if [[ -z "$GROUPNAME" ]]; then
    echo "No groupname is defined, running script with default groupname: 'dev'."
    GROUPNAME="dev"
fi

if [[ -z "$PASSWORD" ]]; then
    echo "No password is defined, running script with default password: 'dev-pass'."
    PASSWORD="dev-pass"
fi

if [[ -z "$ADDRESS" ]]; then
    echo "No specific ip address is defined, running script with default ip: '127.0.0.1'."
    ADDRESS="127.0.0.1"
fi

command -v curl >/dev/null 2>&1 || { echo >&2 "Cluster state script requires curl but it's not installed. Aborting."; exit -1; }

if [[ "$OPERATION" != "get-local-cp-member" ]] && [[ "$OPERATION" != "get-cp-groups" ]] && [[ "$OPERATION" != "get-cp-group" ]] && [[ "$OPERATION" != "force-destroy-cp-group" ]] &&  [[ "$OPERATION" != "get-cp-members" ]] && [[ "$OPERATION" != "promote-to-cp-member" ]] && [[ "$OPERATION" != "remove-cp-member" ]] && [[ "$OPERATION" != "restart" ]] && [[ "$OPERATION" != "get-cp-sessions" ]] && [[ "$OPERATION" != "force-close-cp-session" ]]; then
    echo "Not a valid CP subsystem operation, valid operations are 'get-local-cp-member' | 'get-cp-groups' || 'get-cp-group' || 'force-destroy-cp-group' || 'get-cp-members' || 'promote-to-cp-member' || 'remove-cp-member' || 'restart' || 'get-cp-sessions' || 'force-close-cp-session'"
    exit 0
fi

if [[ "$OPERATION" = "get-local-cp-member" ]]; then
    echo "Getting local CP member information on ${ADDRESS}:${PORT}"
	request="http://${ADDRESS}:${PORT}/hazelcast/rest/cp-subsystem/cp-members/local"
 	response=$(curl -w "\n%{http_code}" --silent "${request}");
    json=$(echo "$response" | head -n 1)
    status_code=$(echo "$response" | tail -n1)

 	if [[ "$status_code" = "200" ]];then
        echo ${json};
    	exit 0
    fi
	if [[ "$status_code" = "404" ]];then
        echo "Not found";
        exit 1
    fi

    echo "Internal error! Status Code: ${status_code} Response: ${response}"
    exit 3
fi

if [[ "$OPERATION" = "get-cp-groups" ]]; then
    echo "Getting CP group IDs on ip ${ADDRESS} on port ${PORT}"
	request="http://${ADDRESS}:${PORT}/hazelcast/rest/cp-subsystem/cp-groups"
 	response=$(curl -w "\n%{http_code}" --silent "${request}");
    json=$(echo "$response" | head -n 1)
    status_code=$(echo "$response" | tail -n1)

 	if [[ "$status_code" = "200" ]];then
        echo ${json};
    	exit 0
    fi

    echo "Internal error! Status Code: ${status_code} Response: ${response}"
    exit 3
fi

if [[ "$OPERATION" = "get-cp-group" ]]; then
    if [[ -z "$CP_GROUP_NAME" ]]; then
        echo "No CP group name is defined! You must provide a CP group name with -c"
        exit 2
    fi

    echo "Getting CP group: ${CP_GROUP_NAME} on ip ${ADDRESS} on port ${PORT}"
	request="http://${ADDRESS}:${PORT}/hazelcast/rest/cp-subsystem/cp-groups/${CP_GROUP_NAME}"
 	response=$(curl -w "\n%{http_code}" --silent "${request}");
    json=$(echo "$response" | head -n 1)
    status_code=$(echo "$response" | tail -n1)

 	if [[ "$status_code" = "200" ]];then
        echo ${json};
    	exit 0
    fi
    if [[ "$status_code" = "404" ]];then
        echo "Not found";
        exit 1
    fi

    echo "Internal error! Status Code: ${status_code} Response: ${response}"
    exit 3
fi

if [[ "$OPERATION" = "get-cp-members" ]]; then
    echo "Getting CP members on ip ${ADDRESS} on port ${PORT}"
	request="http://${ADDRESS}:${PORT}/hazelcast/rest/cp-subsystem/cp-members"
 	response=$(curl -w "\n%{http_code}" --silent "${request}");
    json=$(echo "$response" | head -n 1)
    status_code=$(echo "$response" | tail -n1)

 	if [[ "$status_code" = "200" ]];then
        echo ${json};
    	exit 0
    fi

    echo "Internal error! Status Code: ${status_code} Response: ${response}"
    exit 3
fi

if [[ "$OPERATION" = "get-cp-sessions" ]]; then
    if [[ -z "$CP_GROUP_NAME" ]]; then
        echo "No CP group name is defined! You must provide a CP group name with -c"
        exit 2
    fi

    if [[ "$CP_GROUP_NAME" = "METADATA" ]];then
        echo "Cannot query CP sessions of the METADATA CP group!";
    	exit 2
    fi

    echo "Getting CP sessions in CP group: ${CP_GROUP_NAME} on ip ${ADDRESS} on port ${PORT}"
	request="http://${ADDRESS}:${PORT}/hazelcast/rest/cp-subsystem/cp-groups/${CP_GROUP_NAME}/cp-sessions"
 	response=$(curl -w "\n%{http_code}" --silent "${request}");
    json=$(echo "$response" | head -n 1)
    status_code=$(echo "$response" | tail -n1)

 	if [[ "$status_code" = "200" ]];then
        echo ${json};
    	exit 0
    fi
    if [[ "$status_code" = "404" ]];then
        echo "Not found";
        exit 1
    fi

    echo "Internal error! Status Code: ${status_code} Response: ${response}"
    exit 3
fi

if [[ "$OPERATION" = "force-destroy-cp-group" ]]; then
    if [[ -z "$CP_GROUP_NAME" ]]; then
        echo "No CP group name is defined! You must provide a CP group name with -c"
        exit 2
    fi

    if [[ "$CP_GROUP_NAME" = "METADATA" ]];then
        echo "You cannot force-destroy the METADATA CP group!"
        exit 2
    fi

    echo "Force-destroying CP group: ${CP_GROUP_NAME} on ip ${ADDRESS} on port ${PORT}"
	request="http://${ADDRESS}:${PORT}/hazelcast/rest/cp-subsystem/cp-groups/${CP_GROUP_NAME}/remove"
 	response=$(curl -X POST --data "${GROUPNAME}&${PASSWORD}" -w "\n%{http_code}" --silent "${request}");
    json=$(echo "$response" | head -n 1)
    status_code=$(echo "$response" | tail -n1)

 	if [[ "$status_code" = "200" ]];then
        echo "OK";
    	exit 0
    fi
    if [[ "$status_code" = "400" ]];then
        echo "Bad request";
        exit 1
    fi
    if [[ "$status_code" = "403" ]];then
        echo "Invalid credentials"
    	exit 1
    fi

    echo "Internal error! Status Code: ${status_code} Response: ${response}"
    exit 3
fi

if [[ "$OPERATION" = "promote-to-cp-member" ]]; then
    echo "Promoting to CP member on ip ${ADDRESS} on port ${PORT}"
	request="http://${ADDRESS}:${PORT}/hazelcast/rest/cp-subsystem/cp-members"
 	response=$(curl -X POST --data "${GROUPNAME}&${PASSWORD}" -w "\n%{http_code}" --silent "${request}");
    json=$(echo "$response" | head -n 1)
    status_code=$(echo "$response" | tail -n1)

 	if [[ "$status_code" = "200" ]];then
        echo "OK";
    	exit 0
    fi
    if [[ "$status_code" = "403" ]];then
        echo "Invalid credentials"
    	exit 1
    fi

    echo "Internal error! Status Code: ${status_code} Response: ${response}"
    exit 3
fi

if [[ "$OPERATION" = "remove-cp-member" ]]; then
    if [[ -z "$CP_MEMBER_UID" ]]; then
        echo "No CP member is defined! You must provide a CP member UUID with -m"
        exit 2
    fi

    echo "Removing CP member: ${CP_MEMBER_UID} on ip ${ADDRESS} on port ${PORT}. Please note that you can remove missing CP members only from the master node."
	request="http://${ADDRESS}:${PORT}/hazelcast/rest/cp-subsystem/cp-members/${CP_MEMBER_UID}/remove"
 	response=$(curl -X POST --data "${GROUPNAME}&${PASSWORD}" -w "\n%{http_code}" --silent "${request}");
    json=$(echo "$response" | head -n 1)
    status_code=$(echo "$response" | tail -n1)

 	if [[ "$status_code" = "200" ]];then
        echo "OK";
    	exit 0
    fi
    if [[ "$status_code" = "400" ]];then
        echo "Bad request";
        exit 1
    fi
    if [[ "$status_code" = "403" ]];then
        echo "Invalid credentials"
    	exit 1
    fi

    echo "Internal error! Status Code: ${status_code} Response: ${response}"
    exit 3
fi

if [[ "$OPERATION" = "force-close-cp-session" ]]; then
    if [[ -z "$CP_GROUP_NAME" ]]; then
        echo "No CP group name is defined! You must provide a CP group name with -c"
        exit 2
    fi

    if [[ -z "$CP_SESSION_ID" ]]; then
        echo "No CP session id is defined! You must provide a CP session id with -s"
        exit 2
    fi

    echo "Closing CP session: ${CP_SESSION_ID} in CP group: ${CP_GROUP_NAME} on ip ${ADDRESS} on port ${PORT}"
	request="http://${ADDRESS}:${PORT}/hazelcast/rest/cp-subsystem/cp-groups/${CP_GROUP_NAME}/cp-sessions/${CP_SESSION_ID}/remove"
 	response=$(curl -X POST --data "${GROUPNAME}&${PASSWORD}" -w "\n%{http_code}" --silent "${request}");
    json=$(echo "$response" | head -n 1)
    status_code=$(echo "$response" | tail -n1)

 	if [[ "$status_code" = "200" ]];then
        echo "OK";
    	exit 0
    fi
    if [[ "$status_code" = "400" ]];then
        echo "Bad request";
        exit 1
    fi
    if [[ "$status_code" = "403" ]];then
        echo "Invalid credentials"
    	exit 1
    fi

    echo "Internal error! Status Code: ${status_code} Response: ${response}"
    exit 3
fi

if [[ "$OPERATION" = "restart" ]]; then
    echo "Restarting the CP subsystem on ip ${ADDRESS} on port ${PORT}. Please do not forget to call reset-and-init on all CP nodes!"
	request="http://${ADDRESS}:${PORT}/hazelcast/rest/cp-subsystem/restart"
 	response=$(curl -X POST --data "${GROUPNAME}&${PASSWORD}" -w "\n%{http_code}" --silent "${request}");
    json=$(echo "$response" | head -n 1)
    status_code=$(echo "$response" | tail -n1)

 	if [[ "$status_code" = "200" ]];then
        echo "OK.";
    	exit 0
    fi
    if [[ "$status_code" = "403" ]];then
        echo "Invalid credentials"
    	exit 1
    fi

    echo "Internal error! Status Code: ${status_code} Response: ${response}"
    exit 3
fi
