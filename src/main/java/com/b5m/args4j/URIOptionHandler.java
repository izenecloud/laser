package com.b5m.args4j;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.OptionDef;
import org.kohsuke.args4j.spi.OneArgumentOptionHandler;
import org.kohsuke.args4j.spi.Setter;

import java.net.URI;

public class URIOptionHandler extends OneArgumentOptionHandler<URI> {

    public URIOptionHandler(CmdLineParser parser, OptionDef option, Setter<? super URI> setter) {
        super(parser, option, setter);
    }

    @Override
    protected URI parse(String argument) throws NumberFormatException, CmdLineException {
        return URI.create(argument);
    }
}
