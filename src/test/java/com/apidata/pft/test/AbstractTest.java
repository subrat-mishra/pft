package com.apidata.pft.test;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;

import static org.junit.Assert.assertEquals;

public class AbstractTest {
    public static final String MD5 = "MD5";
    protected File srcFile;
    protected File targetFile;

    @BeforeClass
    public static void init() {
        Thread.currentThread().getContextClassLoader().setDefaultAssertionStatus(true);
    }

    @AfterClass
    public static void destroy() {
        Thread.currentThread().getContextClassLoader().setDefaultAssertionStatus(false);
    }

    protected final void compare() {
        try {
            byte[] b = Files.readAllBytes(Paths.get(srcFile.toURI()));
            byte[] hash = MessageDigest.getInstance(MD5).digest(b);
            String srcSource = DatatypeConverter.printHexBinary(hash);
            b = Files.readAllBytes(Paths.get(srcFile.toURI()));
            hash = MessageDigest.getInstance("MD5").digest(b);
            String destSource = DatatypeConverter.printHexBinary(hash);
            assertEquals("file did not copy completely", srcFile.length(), targetFile.length());
            assertEquals("MD5 comparision is not same src:"+srcFile+" targe: "+targetFile, srcSource, destSource);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
