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

import java.io.IOException;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 10/28/13
 */
public class OAtomicOperationManager {
  private final ThreadLocal<OAtomicOperation> currentOperation = new ThreadLocal<OAtomicOperation>();

  private final OWriteAheadLog                writeAheadLog;

  public OAtomicOperationManager(OWriteAheadLog writeAheadLog) {
    this.writeAheadLog = writeAheadLog;
  }

  public OAtomicOperation startAtomicOperation() throws IOException {
    OAtomicOperation atomicOperation = currentOperation.get();
    if (atomicOperation == null && writeAheadLog != null) {
      final OOperationUnitId unitId = OOperationUnitId.generateId();
      final OLogSequenceNumber lsn = writeAheadLog.log(new OAtomicUnitStartRecord(true, unitId));

      atomicOperation = new OAtomicOperation(unitId, lsn);
      currentOperation.set(atomicOperation);
    }

    if (atomicOperation != null)
      atomicOperation.incrementStartCounter();

    return atomicOperation;
  }

  public boolean endAtomicOperation(boolean rollback) throws IOException {
    final OAtomicOperation atomicOperation = currentOperation.get();

    if (atomicOperation != null) {
      if (rollback)
        atomicOperation.rollback();

      if (atomicOperation.decrementStartCounter() == 0) {
        writeAheadLog.log(new OAtomicUnitEndRecord(atomicOperation.getOperationUnitId(), atomicOperation.isRollback()));
        currentOperation.set(null);

        if (atomicOperation.isRollback() && !rollback)
          throw new ORollbackException(
              "Current operation will be rolled back because, one or several of lower level operations were rolled back.");

        return true;
      }
    }

    return false;
  }

  public OAtomicOperation getCurrentOperation() {
    return currentOperation.get();
  }
}
