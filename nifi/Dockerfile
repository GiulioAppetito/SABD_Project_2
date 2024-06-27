FROM apache/nifi:latest
# Copy flow configuration files in both formats
COPY --chown=nifi:nifi config/NiFi_Flow.xml.gz $NIFI_HOME/conf/flow.xml.gz
COPY --chown=nifi:nifi config/NiFi_Flow.json.gz $NIFI_HOME/conf/flow.json.gz
EXPOSE 8443
EXPOSE 8082
