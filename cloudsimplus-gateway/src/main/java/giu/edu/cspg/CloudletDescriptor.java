package giu.edu.cspg;

import java.util.Objects;

import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.cloudlets.CloudletSimple;
import org.cloudsimplus.util.DataCloudTags;
import org.cloudsimplus.utilizationmodels.UtilizationModelFull;

public class CloudletDescriptor {
    private final int cloudletId;
    private final long submissionDelay;
    private final long mi;
    private final int numberOfCores;
    private int runTime = 1;

    public CloudletDescriptor(int cloudletId, long submissionDelay, long mi, int numberOfCores) {
        this.cloudletId = cloudletId;
        this.submissionDelay = submissionDelay;
        this.mi = mi;
        this.numberOfCores = numberOfCores;
    }

    public CloudletDescriptor(int cloudletId, long submissionDelay, long mi, int numberOfCores, int runTime) {
        this.cloudletId = cloudletId;
        this.submissionDelay = submissionDelay;
        this.mi = mi;
        this.numberOfCores = numberOfCores;
        this.runTime = runTime;
    }

    public int getCloudletId() {
        return cloudletId;
    }

    public long getSubmissionDelay() {
        return submissionDelay;
    }

    public long getMi() {
        return mi;
    }

    public int getNumberOfCores() {
        return numberOfCores;
    }

    public int getRuntime() {
        return runTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        CloudletDescriptor that = (CloudletDescriptor) o;
        return getCloudletId() == that.getCloudletId() &&
                getSubmissionDelay() == that.getSubmissionDelay() &&
                getMi() == that.getMi() &&
                getNumberOfCores() == that.getNumberOfCores() &&
                getRuntime() == that.getRuntime();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getCloudletId(), getSubmissionDelay(), getMi(), getNumberOfCores(), getRuntime());
    }

    @Override
    public String toString() {
        return "CloudletDescriptor{" +
                "cloudletId=" + cloudletId +
                ", submissionDelay=" + submissionDelay +
                ", mi=" + mi +
                ", numberOfCores=" + numberOfCores +
                ", runTime=" + runTime +
                '}';
    }

    public Cloudlet toCloudlet() {
        Cloudlet cloudlet = new CloudletSimple(cloudletId, mi * runTime, numberOfCores)
                .setFileSize(DataCloudTags.DEFAULT_MTU)
                .setOutputSize(DataCloudTags.DEFAULT_MTU)
                .setUtilizationModel(new UtilizationModelFull());
        cloudlet.setSubmissionDelay(submissionDelay);
        return cloudlet;
    }
}
