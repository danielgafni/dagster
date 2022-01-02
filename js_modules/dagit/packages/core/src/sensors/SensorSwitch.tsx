import {gql, useMutation} from '@apollo/client';
import * as React from 'react';

import {DISABLED_MESSAGE, usePermissions} from '../app/Permissions';
import {InstigationStatus} from '../types/globalTypes';
import {Checkbox} from '../ui/Checkbox';
import {Tooltip} from '../ui/Tooltip';
import {repoAddressToSelector} from '../workspace/repoAddressToSelector';
import {RepoAddress} from '../workspace/types';

import {
  displaySensorMutationErrors,
  START_SENSOR_MUTATION,
  STOP_SENSOR_MUTATION,
} from './SensorMutations';
import {SensorSwitchFragment} from './types/SensorSwitchFragment';
import {StartSensor} from './types/StartSensor';
import {StopSensor} from './types/StopSensor';

interface Props {
  repoAddress: RepoAddress;
  sensor: SensorSwitchFragment;
  size?: 'small' | 'large';
}

export const SensorSwitch: React.FC<Props> = (props) => {
  const {repoAddress, sensor, size = 'large'} = props;
  const {canStartSensor, canStopSensor} = usePermissions();

  const {jobOriginId, name, sensorState} = sensor;
  const {status, canChangeStatus} = sensorState;
  const sensorSelector = {
    ...repoAddressToSelector(repoAddress),
    sensorName: name,
  };

  const [startSensor, {loading: toggleOnInFlight}] = useMutation<StartSensor>(
    START_SENSOR_MUTATION,
    {onCompleted: displaySensorMutationErrors},
  );
  const [stopSensor, {loading: toggleOffInFlight}] = useMutation<StopSensor>(STOP_SENSOR_MUTATION, {
    onCompleted: displaySensorMutationErrors,
  });

  const onChangeSwitch = () => {
    if (status === InstigationStatus.RUNNING) {
      stopSensor({variables: {jobOriginId}});
    } else {
      startSensor({variables: {sensorSelector}});
    }
  };

  const running = status === InstigationStatus.RUNNING;

  if (canStartSensor && canStopSensor && canChangeStatus) {
    return (
      <Checkbox
        format="switch"
        disabled={toggleOnInFlight || toggleOffInFlight}
        checked={running || toggleOnInFlight}
        onChange={onChangeSwitch}
        size={size}
      />
    );
  }

  const lacksPermission = (running && !canStartSensor) || (!running && !canStopSensor);
  const disabled = !canChangeStatus || toggleOffInFlight || toggleOnInFlight || lacksPermission;

  const switchElement = (
    <Checkbox
      format="switch"
      disabled={disabled}
      checked={running || toggleOnInFlight}
      onChange={onChangeSwitch}
      size={size}
    />
  );

  let tooltip = null;
  if (!canChangeStatus) {
    if (running) {
      tooltip =
        'Sensor is set as running in code. To stop it, set its status to SensorStatus.STOPPED in code.';
    } else {
      tooltip =
        'Sensor is set as stopped in code. To stop it, set its status to SensorStatus.RUNNING in code.';
    }
  } else if (lacksPermission) {
    tooltip = DISABLED_MESSAGE;
  }

  return tooltip ? <Tooltip content={tooltip}>{switchElement}</Tooltip> : switchElement;
};

export const SENSOR_SWITCH_FRAGMENT = gql`
  fragment SensorSwitchFragment on Sensor {
    id
    jobOriginId
    name
    sensorState {
      id
      status
      canChangeStatus
    }
  }
`;
