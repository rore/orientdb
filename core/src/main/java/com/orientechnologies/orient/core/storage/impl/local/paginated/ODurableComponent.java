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

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptive;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;

/**
 * @author Andrey Lomakin
 * @since 8/27/13
 */
public abstract class ODurableComponent extends OSharedResourceAdaptive {
  private OAtomicOperationManager atomicOperationManager;
  private OWriteAheadLog          writeAheadLog;

  public ODurableComponent() {
  }

  public ODurableComponent(int timeout) {
    super(timeout);
  }

  public ODurableComponent(boolean concurrent) {
    super(concurrent);
  }

  public ODurableComponent(boolean concurrent, int timeout, boolean ignoreThreadInterruption) {
    super(concurrent, timeout, ignoreThreadInterruption);
  }

  protected void init(OAtomicOperationManager atomicOperationManager, OWriteAheadLog writeAheadLog) {
    this.atomicOperationManager = atomicOperationManager;
    this.writeAheadLog = writeAheadLog;
  }

  protected void endDurableOperation(OStorageTransaction transaction, boolean rollback) throws IOException {
    atomicOperationManager.endAtomicOperation(rollback);
  }

  protected OAtomicOperation startDurableOperation(OStorageTransaction transaction) throws IOException {
    return atomicOperationManager.startAtomicOperation();
  }

  protected void logPageChanges(ODurablePage localPage, long fileId, long pageIndex, boolean isNewPage) throws IOException {
    if (writeAheadLog != null) {
      final OPageChanges pageChanges = localPage.getPageChanges();
      if (pageChanges.isEmpty())
        return;

      final OAtomicOperation atomicOperation = atomicOperationManager.getCurrentOperation();

      final OOperationUnitId operationUnitId = atomicOperation.getOperationUnitId();
      final OLogSequenceNumber prevLsn;
      if (isNewPage)
        prevLsn = atomicOperation.getStartLSN();
      else
        prevLsn = localPage.getLsn();

      OLogSequenceNumber lsn = writeAheadLog.log(new OUpdatePageRecord(pageIndex, fileId, operationUnitId, pageChanges, prevLsn));

      localPage.setLsn(lsn);
    }
  }

  protected ODurablePage.TrackMode getTrackMode() {
    final ODurablePage.TrackMode trackMode;

    if (writeAheadLog == null)
      trackMode = ODurablePage.TrackMode.NONE;
    else
      trackMode = ODurablePage.TrackMode.FULL;
    return trackMode;
  }

}
