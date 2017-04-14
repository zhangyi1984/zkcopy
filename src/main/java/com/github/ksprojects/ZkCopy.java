package com.github.ksprojects;

import com.github.ksprojects.zkcopy.Node;
import com.github.ksprojects.zkcopy.reader.Reader;
import com.github.ksprojects.zkcopy.writer.Writer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.log4j.Logger;

public class ZkCopy {

    private static final Logger LOGGER = Logger.getLogger(ZkCopy.class);
    private static final int DEFAULT_THREADS_NUMBER = 10;
    private static final boolean DEFAULT_REMOVE_DEPRECATED_NODES = false;
    private static final boolean DEFAULT_IGNORE_EPHEMERAL_NODES = true;
    private static final String HELP = "help";
    private static final String SOURCE = "source";
    private static final String TARGET = "target";
    private static final String WORKERS = "workers";
    private static final String COPY_ONLY = "copyOnly";
    private static final String IGNORE_EPHEMERAL_NODES = "ignoreEphemeralNodes";

    /**
     * Main entry point - start ZkCopy.
     */
    public static void main(String[] args) {
        Configuration cfg = parseConfiguration(args);
        if (cfg == null) {
            Options options = createOptions();
            printHelp(options);
            return;
        }
        String sourceAddress = cfg.getString(SOURCE);
        String destinationAddress = cfg.getString(TARGET);
        int threads = cfg.getInt(WORKERS);
        boolean removeDeprecatedNodes = cfg.getBoolean(COPY_ONLY);
        LOGGER.info("using " + threads + " concurrent workers to copy data");
        LOGGER.info("delete nodes = " + removeDeprecatedNodes);
        LOGGER.info("ignore ephemeral nodes = " + cfg.getBoolean(IGNORE_EPHEMERAL_NODES));
        Reader reader = new Reader(sourceAddress, threads);
        Node root = reader.read();
        if (root != null) {
            Writer writer = new Writer(destinationAddress, root, removeDeprecatedNodes,
                    cfg.getBoolean(IGNORE_EPHEMERAL_NODES));
            writer.write();
        } else {
            LOGGER.error("FAILED");
        }
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("zkcopy", options);
    }

    private static Configuration parseConfiguration(String[] args) {
        Options options = createOptions();
        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine line = parser.parse(options, args);
            if (line.hasOption(HELP)) {
                printHelp(options);
                return null;
            }
            if (!line.hasOption(SOURCE) || !line.hasOption(TARGET)) {
                return null;
            }
            Configuration configuration = new BaseConfiguration();
            configuration.addProperty(SOURCE, getString(line, SOURCE));
            configuration.addProperty(TARGET, getString(line, TARGET));
            configuration.addProperty(WORKERS, getInteger(line, WORKERS, DEFAULT_THREADS_NUMBER));
            configuration.addProperty(COPY_ONLY,
                    getBoolean(line, COPY_ONLY, DEFAULT_REMOVE_DEPRECATED_NODES));
            configuration.addProperty(IGNORE_EPHEMERAL_NODES,
                    getBoolean(line, IGNORE_EPHEMERAL_NODES, DEFAULT_IGNORE_EPHEMERAL_NODES));
            return configuration;
        } catch (ParseException exp) {
            LOGGER.error("Could not parse options.  Reason: " + exp.getMessage());
            return null;
        }
    }

    private static Options createOptions() {
        Options options = new Options();

        Option help = Option.builder("h").longOpt(HELP).desc("print this message").build();
        Option source = Option.builder("s").longOpt(SOURCE).hasArg().argName("server:port/path")
                .desc("location of a source tree to copy").build();
        Option target = Option.builder("t").longOpt(TARGET).hasArg().argName("server:port/path")
                .desc("target location").build();
        Option workers = Option.builder("w").longOpt(WORKERS).hasArg().argName("N")
                .desc("(optional) number of concurrent workers to copy data").build();
        Option copyOnly = Option.builder("c").longOpt(COPY_ONLY).hasArg().argName("true|false")
                .desc("(optional) set this flag if you do not want to remove nodes that are removed on source")
                .build();
        Option ignoreEphemeralNodes =
                Option.builder("i").longOpt(IGNORE_EPHEMERAL_NODES).hasArg().argName("true|false")
                        .desc("(optional) set this flag if you do not want to copy ephemeral ZNodes")
                        .build();

        options.addOption(help);
        options.addOption(source);
        options.addOption(target);
        options.addOption(workers);
        options.addOption(copyOnly);
        options.addOption(ignoreEphemeralNodes);
        return options;
    }

    private static String getString(CommandLine line, String name) {
        return line.getOptionValue(name);
    }

    private static int getInteger(CommandLine line, String name, int defaultValue) {
        try {
            String value = line.getOptionValue(name);
            if (value == null) {
                return defaultValue;
            }
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            LOGGER.warn("Could not parse option " + name + ": " + e.getMessage());
            return defaultValue;
        }
    }

    private static boolean getBoolean(CommandLine line, String name, boolean defaultValue) {
        try {
            String value = line.getOptionValue(name);
            if (value == null) {
                return defaultValue;
            }
            return Boolean.parseBoolean(value);
        } catch (NumberFormatException e) {
            LOGGER.warn("Could not parse option " + name + ": " + e.getMessage());
            return defaultValue;
        }
    }

}


