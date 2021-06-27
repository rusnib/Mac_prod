# /sas_viya_backup

# Before run this script make sure that the default connection profile for SAS Viya Administrative CLI exists

# ================================== #
#    Run default SAS Viya Backup     #
# ================================== #

# Set Authentication options
export SAS_USER=sasboot
export SAS_PASSWORD=Idnleimd

# Export requered Environment Variables
export ADMIN_CLI_PATH=/opt/sas/viya/home/bin
. /opt/sas/viya/config/consul.conf


# Login to SAS Viya Administrative CLI
$ADMIN_CLI_PATH/sas-admin auth login -u $SAS_USER -p $SAS_PASSWORD

# Start SAS Viya Backup
$ADMIN_CLI_PATH/sas-admin backup start
# Backup has been started. Check status in SAS Environment Manager - Backup and Restore.

# Logout from SAS Viya Administrative CLI
$ADMIN_CLI_PATH/sas-admin auth logout

unset SAS_USER
unset SAS_PASSWORD
unset ADMIN_CLI_PATH

# =================================== #
#   Run promotool PostgreSQL backup   #
# sasdevinf.ru-central1.internal:5452 #
# =================================== #
# ETL DB
#export PGHOST=sasdevinf.ru-central1.internal
export PGHOST=rumskap101.ru-central1.internal
export PGPORT=5452
export PGUSER=sas
export PGDBNAME=pt
export PGPASSWORD="Orion123"
export PGDUMP_OPTIONS="--exclude-schema=backup --format=custom --verbose --lock-wait-timeout 300000"

export BACKUP_PATH=/data/sas_backup/postgresql/$PGHOST_$PGPORT
if [ ! -d "$BACKUP_PATH" ] 
then
    mkdir -p $BACKUP_PATH
fi

/opt/sas/viya/home/postgresql11/bin/pg_dump postgresql://$PGUSER:$PGPASSWORD@$PGHOST:$PGPORT/$PGDBNAME $PGDUMP_OPTIONS -f $BACKUP_PATH/pg_dump_$PGDBNAME_`date +%Y-%m-%dT%H-%M-%S`.dmp

# PT DB
export PGDBNAME=pt
/opt/sas/viya/home/postgresql11/bin/pg_dump postgresql://$PGUSER:$PGPASSWORD@$PGHOST:$PGPORT/$PGDBNAME $PGDUMP_OPTIONS -f $BACKUP_PATH/pg_dump_$PGDBNAME_`date +%Y-%m-%dT%H-%M-%S`.dmp


unset PGHOST
unset PGPORT
unset PGUSER
unset PGDBNAME
unset PGPASSWORD
unset PGDUMP_OPTIONS

# ================================== #
#  Run integration scripts backup    #
# /opt/sas/mcd_config                #
# ================================== #
# Export requered Environment Variables
export BACKUP_SOURCE_DIR=/opt/sas/
export BACKUP_SOURCE_NAME=mcd_config
export BACKUP_PATH=/data/sas_backup/$BACKUP_SOURCE_NAME

if [ ! -d "$BACKUP_PATH" ] 
then
    mkdir -p $BACKUP_PATH
fi

# Run backup
tar czf $BACKUP_PATH/${BACKUP_SOURCE_NAME}_`date +%Y-%m-%dT%H-%M-%S`.tgz -C $BACKUP_SOURCE_DIR $BACKUP_SOURCE_NAME

unset BACKUP_SOURCE_DIR
unset BACKUP_SOURCE_NAME
unset BACKUP_PATH


# ================================== #
#  Run configuration files backup    #
# /data/files                        #
# ================================== #
# Export requered Environment Variables
export BACKUP_SOURCE_DIR=/data
export BACKUP_SOURCE_NAME=files
export BACKUP_PATH=/data/sas_backup/$BACKUP_SOURCE_NAME

if [ ! -d "$BACKUP_PATH" ] 
then
    mkdir -p $BACKUP_PATH
fi

# Run backup
tar czf $BACKUP_PATH/${BACKUP_SOURCE_NAME}_`date +%Y-%m-%dT%H-%M-%S`.tgz -C $BACKUP_SOURCE_DIR $BACKUP_SOURCE_NAME

unset BACKUP_SOURCE_DIR
unset BACKUP_SOURCE_NAME
unset BACKUP_PATH


#EOF