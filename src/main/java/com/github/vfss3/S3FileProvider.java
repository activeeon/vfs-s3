package com.github.vfss3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Region;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * An S3 file provider. Create an S3 file system out of an S3 file name. Also
 * defines the capabilities of the file system.
 *
 * @author Marat Komarov
 * @author Matthias L. Jugel
 * @author Moritz Siuts
 */
public class S3FileProvider extends AbstractOriginatingFileProvider {
    public final static Collection<Capability> capabilities = Collections.unmodifiableCollection(Arrays.asList(
        Capability.CREATE,
        Capability.DELETE,
        Capability.GET_TYPE,
        Capability.GET_LAST_MODIFIED,
        Capability.SET_LAST_MODIFIED_FILE,
        Capability.SET_LAST_MODIFIED_FOLDER,
        Capability.LIST_CHILDREN,
        Capability.READ_CONTENT,
        Capability.URI,
        Capability.WRITE_CONTENT
    ));

    public S3FileProvider() {
        setFileNameParser(S3FileNameParser.getInstance());
    }

    /**
     * Create a file system with the S3 root provided.
     *
     * @param fileName the S3 file name that defines the root (bucket)
     * @param fileSystemOptions file system options
     * @return an S3 file system
     * @throws FileSystemException if the file system cannot be created
     */
    @Override
    protected FileSystem doCreateFileSystem(
            FileName fileName, FileSystemOptions fileSystemOptions
    ) throws FileSystemException {
        final S3FileSystemOptions options = new S3FileSystemOptions(fileSystemOptions);
        AmazonS3Client service = options.getS3Client();
        if (service == null) {
            if (DEFAULT_CLIENT != null) {
                service = DEFAULT_CLIENT;
            } else {
                ClientConfiguration clientConfiguration = options.getClientConfiguration();
                AmazonS3Client s3 = new AmazonS3Client(new DefaultAWSCredentialsProviderChain(), clientConfiguration);

                Region region = options.getRegion();
                if (region != null) {
                    s3.setRegion(region.toAWSRegion());
                }

                service = s3;
            }
        }

        S3FileSystem fileSystem = new S3FileSystem((S3FileName) fileName, service, options);

        if (options.getS3Client() != null) {
            fileSystem.setShutdownServiceOnClose(true);
        }

        return fileSystem;
    }

    /**
     * Get the capabilities of the file system provider.
     *
     * @return the file system capabilities
     */
    @Override
    public Collection<Capability> getCapabilities() {
        return capabilities;
    }

    private static AmazonS3Client DEFAULT_CLIENT = null;

    /**
     * commons-vfs doen't support default options so we have to do something with default S3 client.
     *
     * @param client it will be used in case of no client was specified.
     */
    public static void setDefaultClient(AmazonS3Client client) {
        DEFAULT_CLIENT = client;
    }
}
