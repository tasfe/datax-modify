summary: DataX hdfswriter can write data to hadoop-hdfs 
Name: t_dp_datax_hdfswriter
Version: 1.0.0
Release: 1
Group: System
License: GPL
BuildArch: noarch
AutoReqProv: no 
Requires: t_dp_datax_engine

%define dataxpath /home/taobao/datax

%description
DataX hdfswriter can write data to hadoop-hdfs 

%prep
cd ${OLDPWD}/../
export LANG=zh_CN.UTF-8
ant dist

%build

%install
mkdir -p %{dataxpath}/plugins/writer/hdfswriter

cp ${OLDPWD}/../src/com/taobao/datax/plugins/writer/hdfswriter/ParamKey.java %{dataxpath}/plugins/writer/hdfswriter
cp ${OLDPWD}/../build/plugins/hdfswriter-1.0.0.jar %{dataxpath}/plugins/writer/hdfswriter
cp ${OLDPWD}/../build/plugins/plugins-common-1.0.0.jar %{dataxpath}/plugins/writer/hdfswriter
cp -r ${OLDPWD}/../libs/hadoop-0.19.2-core.jar %{dataxpath}/plugins/writer/hdfswriter
cp -r ${OLDPWD}/../libs/commons-logging-1.1.1.jar %{dataxpath}/plugins/writer/hdfswriter
cp -r ${OLDPWD}/../libs/libhadoop.so %{dataxpath}/plugins/writer/hdfswriter

%files
%defattr(0755,root,root)
%{dataxpath}/plugins/writer/hdfswriter

%changelog
* Fri Aug 20 2010 meining 
- Version 1.0.0

