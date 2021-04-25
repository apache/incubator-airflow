/* eslint-disable no-console */
/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import axios, { AxiosResponse } from 'axios';
import {
  useMutation, useQuery, useQueryClient, setLogger,
} from 'react-query';
import humps from 'humps';
import { useToast } from '@chakra-ui/react';

import type {
  Config, Dag, DagRun, Version,
} from 'interfaces';
import type {
  DagsResponse,
  DagRunsResponse,
  TaskInstancesResponse,
  TriggerRunRequest,
} from 'interfaces/api';

axios.defaults.baseURL = `${process.env.WEBSERVER_URL}/api/v1`;
axios.interceptors.response.use(
  (res) => (res.data ? humps.camelizeKeys(res.data) as unknown as AxiosResponse : res),
);

// turn off logging, retry and refetch on tests
const isTest = process.env.NODE_ENV === 'test';

setLogger({
  log: isTest ? () => {} : console.log,
  warn: isTest ? () => {} : console.warn,
  error: isTest ? () => {} : console.warn,
});

const toastDuration = 3000;
const refetchInterval = isTest ? false : 1000;

export function useDags() {
  return useQuery<DagsResponse, Error>(
    'dags',
    (): Promise<DagsResponse> => axios.get('/dags'),
    {
      refetchInterval,
      retry: !isTest,
    },
  );
}

export function useDagRuns(dagId: Dag['dagId'], dateMin?: string) {
  return useQuery<DagRunsResponse, Error>(
    ['dagRun', dagId],
    (): Promise<DagRunsResponse> => axios.get(`dags/${dagId}/dagRuns${dateMin ? `?start_date_gte=${dateMin}` : ''}`),
    { refetchInterval },
  );
}

export function useTaskInstances(dagId: Dag['dagId'], dagRunId: DagRun['dagRunId'], dateMin?: string) {
  return useQuery<TaskInstancesResponse, Error>(
    ['taskInstance', dagRunId],
    (): Promise<TaskInstancesResponse> => (
      axios.get(`dags/${dagId}/dagRuns/${dagRunId}/taskInstances${dateMin ? `?start_date_gte=${dateMin}` : ''}`)
    ),
  );
}

export function useVersion() {
  return useQuery<Version, Error>(
    'version',
    (): Promise<Version> => axios.get('/version'),
  );
}

export function useConfig() {
  return useQuery<Config, Error>('config', (): Promise<Config> => axios.get('/config'));
}

export function useTriggerRun(dagId: Dag['dagId']) {
  const queryClient = useQueryClient();
  const toast = useToast();
  return useMutation(
    (trigger: TriggerRunRequest) => axios.post(`dags/${dagId}/dagRuns`, humps.decamelizeKeys(trigger)),
    {
      onSettled: (res, error) => {
        if (error) {
          toast({
            title: 'Error triggering DAG',
            description: (error as Error).message,
            status: 'error',
            duration: toastDuration,
            isClosable: true,
          });
        } else {
          toast({
            title: 'DAG Triggered',
            status: 'success',
            duration: toastDuration,
            isClosable: true,
          });
          const dagRunData = queryClient.getQueryData(['dagRun', dagId]) as unknown as DagRunsResponse;
          if (dagRunData) {
            queryClient.setQueryData(['dagRun', dagId], {
              dagRuns: [...dagRunData.dagRuns, res],
              totalEntries: dagRunData.totalEntries += 1,
            });
          } else {
            queryClient.setQueryData(['dagRun', dagId], {
              dagRuns: [res],
              totalEntries: 1,
            });
          }
        }
        queryClient.invalidateQueries(['dagRun', dagId]);
      },
    },
  );
}

export function useSaveDag(dagId: Dag['dagId']) {
  const queryClient = useQueryClient();
  const toast = useToast();
  return useMutation(
    (updatedValues: Record<string, any>) => axios.patch(`dags/${dagId}`, humps.decamelizeKeys(updatedValues)),
    {
      onMutate: async (updatedValues: Record<string, any>) => {
        await queryClient.cancelQueries(['dag', dagId]);
        const previousDag = queryClient.getQueryData(['dag', dagId]) as Dag;
        const previousDags = queryClient.getQueryData('dags') as DagsResponse;

        const newDags = previousDags.dags.map((dag) => (
          dag.dagId === dagId ? { ...dag, ...updatedValues } : dag
        ));
        const newDag = {
          ...previousDag,
          ...updatedValues,
        };

        // optimistically set the dag before the async request
        queryClient.setQueryData(['dag', dagId], () => newDag);
        queryClient.setQueryData('dags', (old) => ({
          ...(old as Dag[]),
          ...{
            dags: newDags,
            totalEntries: previousDags.totalEntries,
          },
        }));
        return { [dagId]: previousDag, dags: previousDags };
      },
      onSettled: (res, error, variables, context) => {
        const previousDag = (context as any)[dagId] as Dag;
        const previousDags = (context as any).dags as DagsResponse;
        // rollback to previous cache on error
        if (error) {
          queryClient.setQueryData(['dag', dagId], previousDag);
          queryClient.setQueryData('dags', previousDags);
          toast({
            title: 'Error updating pipeline',
            description: (error as Error).message,
            status: 'error',
            duration: toastDuration,
            isClosable: true,
          });
        } else {
          // check if server response is different from our optimistic update
          if (JSON.stringify(res) !== JSON.stringify(previousDag)) {
            queryClient.setQueryData(['dag', dagId], res);
            queryClient.setQueryData('dags', {
              dags: previousDags.dags.map((dag) => (
                dag.dagId === dagId ? res : dag
              )),
              totalEntries: previousDags.totalEntries,
            });
          }
          toast({
            title: 'Pipeline Updated',
            status: 'success',
            duration: toastDuration,
            isClosable: true,
          });
        }
        queryClient.invalidateQueries(['dag', dagId]);
      },
    },
  );
}
