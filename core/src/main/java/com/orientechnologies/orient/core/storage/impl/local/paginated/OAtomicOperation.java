/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 10/28/13
 */
public class OAtomicOperation {
  private final OOperationUnitId   operationUnitId;
  private final OLogSequenceNumber startLSN;

  private int                      startCounter;
  private boolean                  rollback;

  public OAtomicOperation(OOperationUnitId operationUnitId, OLogSequenceNumber startLSN) {
    this.operationUnitId = operationUnitId;
    this.startLSN = startLSN;
  }

  public OOperationUnitId getOperationUnitId() {
    return operationUnitId;
  }

  public OLogSequenceNumber getStartLSN() {
    return startLSN;
  }

  void rollback() {
    rollback = true;
  }

  boolean isRollback() {
    return rollback;
  }

  void incrementStartCounter() {
    startCounter++;
  }

  int decrementStartCounter() {
    final int result = --startCounter;
    assert result >= 0;
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OAtomicOperation that = (OAtomicOperation) o;

    if (!operationUnitId.equals(that.operationUnitId))
      return false;
    if (!startLSN.equals(that.startLSN))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = operationUnitId.hashCode();
    result = 31 * result + startLSN.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "OAtomicOperation{" + "operationUnitId=" + operationUnitId + ", startLSN=" + startLSN + '}';
  }
}
