#!/bin/bash

# Segment 1
unshare --net --uts /bin/bash &
export pid_netnsB1=$!
unshare --net --uts /bin/bash &
export pid_netns11=$!
unshare --net --uts /bin/bash &
export pid_netns12=$!

# Segment 2
unshare --net --uts /bin/bash &
export pid_netnsB2=$!
unshare --net --uts /bin/bash &
export pid_netns21=$!
unshare --net --uts /bin/bash &
export pid_netns22=$!

# Router
unshare --net --uts /bin/bash &
export pid_netnsR=$!


# Assign different hostnames
# ~~~~~~~~~~~~~~~~~~~~~~~~~~
nsenter -t $pid_netnsB1 -u hostname netnsB1
nsenter -t $pid_netns11 -u hostname netns11
nsenter -t $pid_netns12 -u hostname netns12

nsenter -t $pid_netnsB2 -u hostname netnsB2
nsenter -t $pid_netns21 -u hostname netns21
nsenter -t $pid_netns22 -u hostname netns22

nsenter -t $pid_netnsR -u hostname netnsR


# Assign NICs
# ~~~~~~~~~~~~~~~~~~~~~~~~~~

#set up veth devices in netns11, netns12  with connections to netnsB1
ip link add veth11 netns $pid_netns11 type veth peer name veth11B netns $pid_netnsB1
ip link add veth12 netns $pid_netns12 type veth peer name veth12B netns $pid_netnsB1

#set up veth devices in netns21, netns22  with connections to netns2B
ip link add veth21 netns $pid_netns21 type veth peer name veth21B netns $pid_netnsB2
ip link add veth22 netns $pid_netns22 type veth peer name veth22B netns $pid_netnsB2

# Conect netnsR to the bridges via 2 veths with end NICs in netnsR
ip link add veth1R netns $pid_netnsR type veth peer name veth1RB netns $pid_netnsB1
ip link add veth2R netns $pid_netnsR type veth peer name veth2RB netns $pid_netnsB2


# Assign IPs
# ~~~~~~~~~~~~~~~~~~~~~~~~~~
echo "original bash: " $$
echo "<< working in netns11"
nsenter -t $pid_netns11 -u -n ip addr add 192.168.5.1/24 brd 192.168.5.255 dev veth11
nsenter -t $pid_netns11 -u -n ip link set veth11 up
nsenter -t $pid_netns11 -u -n ip link set lo up
nsenter -t $pid_netns11 -u -n ip a
echo ">> "
echo "original bash: " $$
echo ""
echo "<< working in netns12"
nsenter -t $pid_netns12 -u -n ip addr add 192.168.5.11/24 brd 192.168.5.255 dev veth12
nsenter -t $pid_netns12 -u -n ip link set veth12 up
nsenter -t $pid_netns12 -u -n ip link set lo up
nsenter -t $pid_netns12 -u -n ip a
echo ">> "
echo "original bash: " $$
echo ""
echo "<< working in netns21"
nsenter -t $pid_netns21 -u -n ip addr add 192.168.5.2/24 brd 192.168.5.255 dev veth21
nsenter -t $pid_netns21 -u -n ip link set veth21 up
nsenter -t $pid_netns21 -u -n ip link set lo up
nsenter -t $pid_netns21 -u -n ip a
echo ">> "
echo "original bash: " $$
echo ""
echo "<< working in netns22"
nsenter -t $pid_netns22 -u -n ip addr add 192.168.5.22/24 brd 192.168.5.255 dev veth22
nsenter -t $pid_netns22 -u -n ip link set veth22 up
nsenter -t $pid_netns22 -u -n ip link set lo up
nsenter -t $pid_netns22 -u -n ip a
echo ">> "
echo "original bash: " $$
echo ""
echo "<< working in netnsR"
nsenter -t $pid_netnsR -u -n ip addr add 192.168.5.100/24 brd 192.168.5.255 dev veth1R
nsenter -t $pid_netnsR -u -n ip link set veth1R up
nsenter -t $pid_netnsR -u -n ip addr add 192.168.5.200/24 brd 192.168.5.255 dev veth2R
nsenter -t $pid_netnsR -u -n ip link set veth2R up
nsenter -t $pid_netnsR -u -n ip link set lo up
nsenter -t $pid_netnsR -u -n ip a

# The extended part which overwrites automatically set rules
echo "Changing routes"
nsenter -t $pid_netnsR -u -n ip route del  192.168.5.0/24  dev veth1R
nsenter -t $pid_netnsR -u -n ip route del  192.168.5.0/24  dev veth2R
nsenter -t $pid_netnsR -u -n ip route add  192.168.5.1 via 192.168.5.100 dev veth1R
nsenter -t $pid_netnsR -u -n ip route add  192.168.5.11 via 192.168.5.100 dev veth1R
nsenter -t $pid_netnsR -u -n ip route add  192.168.5.2 via 192.168.5.200 dev veth2R
nsenter -t $pid_netnsR -u -n ip route add  192.168.5.22 via 192.168.5.200 dev veth2R
nsenter -t $pid_netnsR -u -n route

echo ">> "
echo "original bash: " $$
echo ""

# Setting up the bridges
# ~~~~~~~~~~~~~~~~~~~~~~

# Bridge B1
echo "<< working in netnsB1"
nsenter -t $pid_netnsB1 -u -n brctl addbr br1
nsenter -t $pid_netnsB1 -u -n ip link set br1 up
nsenter -t $pid_netnsB1 -u -n ip link set veth11B up
nsenter -t $pid_netnsB1 -u -n ip link set veth12B up
nsenter -t $pid_netnsB1 -u -n ip link set veth1RB up
nsenter -t $pid_netnsB1 -u -n ip link set lo up
nsenter -t $pid_netnsB1 -u -n brctl addif br1 veth11B
nsenter -t $pid_netnsB1 -u -n brctl addif br1 veth12B
nsenter -t $pid_netnsB1 -u -n brctl addif br1 veth1RB
nsenter -t $pid_netnsB1 -u -n ip a
echo ">> "
echo "original bash: " $$
echo ""

# Bridge B2
echo "<< working in netnsB2"
nsenter -t $pid_netnsB2 -u -n brctl addbr br2
nsenter -t $pid_netnsB2 -u -n ip link set br2 up
nsenter -t $pid_netnsB2 -u -n ip link set veth21B up
nsenter -t $pid_netnsB2 -u -n ip link set veth22B up
nsenter -t $pid_netnsB2 -u -n ip link set veth2RB up
nsenter -t $pid_netnsB2 -u -n ip link set lo up
nsenter -t $pid_netnsB2 -u -n brctl addif br2 veth21B
nsenter -t $pid_netnsB2 -u -n brctl addif br2 veth22B
nsenter -t $pid_netnsB2 -u -n brctl addif br2 veth2RB
nsenter -t $pid_netnsB2 -u -n ip a
echo ">> "
echo "original bash: " $$
echo ""

# Starting konsole windows on KDE desktop
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Change the terminal emulation for other desktops

echo "<< Starting konsoles to enter namespaces"
konsole &>/dev/null --profile "Root Shell" -e "nsenter -t $pid_netns11 -u -n /bin/bash"  &
echo "Started konsole for netns11 PID $!"
export pid_kon_netns11=$!
konsole &>/dev/null --profile "Root Shell" -e "nsenter -t $pid_netns12 -u -n /bin/bash"  &
echo "Started konsole for netns12 PID $!"
export pid_kon_netns12=$!
konsole &>/dev/null --profile "Root Shell" -e "nsenter -t $pid_netns21 -u -n /bin/bash" &
echo "Started konsole for netns21 PID $!"
export pid_kon_netns21=$!
konsole &>/dev/null --profile "Root Shell" -e "nsenter -t $pid_netns22 -u -n /bin/bash" &
echo "Started konsole for netns22 PID $!"
export pid_kon_netns22=$!
konsole &>/dev/null --profile "Root Shell" -e "nsenter -t $pid_netnsR -u -n /bin/bash" &
echo "Started konsole for netnsR PID $!"
export pid_kon_netnsR=$!
echo ">> "
echo "original bash: " $$
echo ""
