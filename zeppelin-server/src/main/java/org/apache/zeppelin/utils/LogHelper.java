package org.apache.zeppelin.utils;

import org.apache.log4j.*;
import org.apache.zeppelin.rest.ResultExportHookRestApi;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

/**
 * 定向日志输出
 * @author xiejiajun
 */
public class LogHelper {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ResultExportHookRestApi.class);

    /**
     * 构造Logger
     * @param logFileName
     * @param logPath
     * @return
     */
    public static Logger createLogger(String logFileName,String logPath) {
        // Create logger
        final String loggerName =  "custom-logger-" + UUID.randomUUID();
        Logger logger = Logger.getLogger(loggerName);
        FileAppender fileAppender;
        try {
            fileAppender = createFileAppender(logFileName,logPath);
            logger.addAppender(fileAppender);
            logger.setAdditivity(false);
        } catch (final IOException e) {
            LOG.error("create custom logger error {}",e.getMessage());
        }
        return logger;
    }




    private static FileAppender createFileAppender(String logFileName,String logPath) throws IOException {
        // Set up log files
        File logFile = new File(logPath, logFileName);
        final String absolutePath = logFile.getAbsolutePath();

        // Attempt to create FileAppender
        Layout loggerLayout =  new EnhancedPatternLayout("%d{dd-MM-yyyy HH:mm:ss z} -| %m\n");
        final RollingFileAppender fileAppender =
                new RollingFileAppender(loggerLayout, absolutePath, true);

        fileAppender.setMaxBackupIndex(4);
        fileAppender.setMaxFileSize("5MB");

        return fileAppender;
    }
}
