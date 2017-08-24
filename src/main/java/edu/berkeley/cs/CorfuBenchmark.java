package edu.berkeley.cs;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;

import java.util.Map;

class CorfuBenchmark {
    private CorfuRuntime corfuRuntime;
    private Map<Long, String> map;

    @SuppressWarnings("deprecation")
    CorfuBenchmark(String corfuConfigurationString, String streamName) {
        setCorfuRuntime(getRuntimeAndConnect(corfuConfigurationString));
        setMap(getCorfuRuntime()
                .getObjectsView()
                .build()
                .setStreamName(streamName)
                .setType(SMRMap.class)
                .open());
    }

    private CorfuRuntime getRuntimeAndConnect(String configurationString) {
        return new CorfuRuntime(configurationString).connect();
    }

    CorfuRuntime getCorfuRuntime() {

        return corfuRuntime;
    }

    private void setCorfuRuntime(CorfuRuntime corfuRuntime) {
        this.corfuRuntime = corfuRuntime;
    }

    Map<Long, String> getMap() {
        return map;
    }

    private void setMap(Map<Long, String> map) {
        this.map = map;
    }
}
