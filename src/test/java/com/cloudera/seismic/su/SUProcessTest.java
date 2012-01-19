/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.seismic.su;

import com.cloudera.seismic.su.SUProcess;

import junit.framework.TestCase;

/**
 * @author josh
 *
 */
public class SUProcessTest extends TestCase {
  public void testHelp() throws Exception {
    SUProcess proc = new SUProcess("/usr/local/su", "susort");
    proc.start();
    System.out.println(proc.closeAndWait());
  }
}
