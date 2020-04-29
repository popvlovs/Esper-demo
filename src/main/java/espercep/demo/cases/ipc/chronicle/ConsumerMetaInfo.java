package espercep.demo.cases.ipc.chronicle;

import java.io.Serializable;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/4/7
 */
public class ConsumerMetaInfo implements Serializable {

    private String id;
    private Long index;
    private Long heartbeat;
    private String acquiredStoreFile;
    private String releasedStoreFile;

    public String getAcquiredStoreFile() {
        return acquiredStoreFile;
    }

    public void setAcquiredStoreFile(String acquiredStoreFile) {
        this.acquiredStoreFile = acquiredStoreFile;
    }

    public String getReleasedStoreFile() {
        return releasedStoreFile;
    }

    public void setReleasedStoreFile(String releasedStoreFile) {
        this.releasedStoreFile = releasedStoreFile;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getIndex() {
        return index;
    }

    public void setIndex(Long index) {
        this.index = index;
    }

    public Long getHeartbeat() {
        return heartbeat;
    }

    public void setHeartbeat(Long heartbeat) {
        this.heartbeat = heartbeat;
    }

    public void mergeIfPresent(ConsumerMetaInfo rhs) {
        ConsumerMetaInfo lhs = this;
        if (rhs.getIndex() != null) {
            lhs.setIndex(rhs.getIndex());
        }
        if (rhs.getHeartbeat() != null) {
            lhs.setHeartbeat(rhs.getHeartbeat());
        }
        if (rhs.getAcquiredStoreFile() != null) {
            lhs.setAcquiredStoreFile(rhs.getAcquiredStoreFile());
        }
        if (rhs.getReleasedStoreFile() != null) {
            lhs.setReleasedStoreFile(rhs.getReleasedStoreFile());
        }
    }

    @Override
    public String toString() {
        return "ConsumerMetaInfo{" +
                "id='" + id + '\'' +
                ", index=" + index +
                ", heartbeat=" + heartbeat +
                ", acquiredStoreFile='" + acquiredStoreFile + '\'' +
                ", releasedStoreFile='" + releasedStoreFile + '\'' +
                '}';
    }
}
