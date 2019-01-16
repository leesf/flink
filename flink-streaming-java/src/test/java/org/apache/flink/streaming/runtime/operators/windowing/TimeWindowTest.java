/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;

/**
 * Tests for {@link TimeWindow}.
 */
public class TimeWindowTest {
	@Test
	public void testGetWindowStartWithOffset() {
		// [0, 7), [7, 14), [14, 21)...
		long offset = 0;
		Assert.assertEquals(TimeWindow.getWindowStartWithOffset(1, offset, 7), 0);
		Assert.assertEquals(TimeWindow.getWindowStartWithOffset(6, offset, 7), 0);
		Assert.assertEquals(TimeWindow.getWindowStartWithOffset(7, offset, 7), 7);
		Assert.assertEquals(TimeWindow.getWindowStartWithOffset(8, offset, 7), 7);

		// [-4, 3), [3, 10), [10, 17)...
		offset = 3;
		Assert.assertEquals(TimeWindow.getWindowStartWithOffset(1, offset, 7), -4);
		Assert.assertEquals(TimeWindow.getWindowStartWithOffset(2, offset, 7), -4);
		Assert.assertEquals(TimeWindow.getWindowStartWithOffset(3, offset, 7), 3);
		Assert.assertEquals(TimeWindow.getWindowStartWithOffset(9, offset, 7), 3);
		Assert.assertEquals(TimeWindow.getWindowStartWithOffset(10, offset, 7), 10);

		// [-2, 5), [5, 12), [12, 19)...
		offset = -2;
		Assert.assertEquals(TimeWindow.getWindowStartWithOffset(1, offset, 7), -2);
		Assert.assertEquals(TimeWindow.getWindowStartWithOffset(-2, offset, 7), -2);
		Assert.assertEquals(TimeWindow.getWindowStartWithOffset(3, offset, 7), -2);
		Assert.assertEquals(TimeWindow.getWindowStartWithOffset(4, offset, 7), -2);
		Assert.assertEquals(TimeWindow.getWindowStartWithOffset(7, offset, 7), 5);
		Assert.assertEquals(TimeWindow.getWindowStartWithOffset(12, offset, 7), 12);

		// for GMT+8:00
		offset = -TimeUnit.HOURS.toMillis(8);
		long size = TimeUnit.DAYS.toMillis(1);
		Assert.assertEquals(TimeWindow.getWindowStartWithOffset(1470902048450L, offset, size), 1470844800000L);
	}

	@Test
	public void testMergeWindows() {
		// window[2, 6), window[3, 5), window[4, 7), window[8, 10)
		MergingWindowAssigner.MergeCallback callback = mock(MergingWindowAssigner.MergeCallback.class);
		TimeWindow timeWindow1 = new TimeWindow(2, 6);
		TimeWindow timeWindow2 = new TimeWindow(3, 5);
		TimeWindow timeWindow3 = new TimeWindow(4, 7);
		TimeWindow timeWindow4 = new TimeWindow(8, 10);

		List<TimeWindow> list = new ArrayList<>();
		list.add(timeWindow1);
		list.add(timeWindow2);
		list.add(timeWindow3);
		list.add(timeWindow4);


		TimeWindow.mergeWindows(list, callback);

	}


}
