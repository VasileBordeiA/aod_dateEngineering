FROM grafana/grafana-oss:12.4.3
# Bake the plugin in: the venue has no internet at runtime (spec 05).
ENV GF_PATHS_PLUGINS=/opt/grafana-plugins
USER root
RUN mkdir -p /opt/grafana-plugins \
 && grafana cli --pluginsDir /opt/grafana-plugins plugins install grafana-clickhouse-datasource 4.21.3 \
 && chown -R 472:0 /opt/grafana-plugins
USER 472
