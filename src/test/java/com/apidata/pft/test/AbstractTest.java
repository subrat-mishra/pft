package com.apidata.pft.test;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class AbstractTest {

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
        assertEquals("file did not copy completely", this.srcFile.length(), this.targetFile.length());
    }
}
