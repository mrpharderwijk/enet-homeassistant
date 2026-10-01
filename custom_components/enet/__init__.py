"""The Enet Smart Home integration."""

from __future__ import annotations

import logging
import asyncio
import aiohttp
import time
import random

from typing import Any, Dict, NoReturn

from .enet_data.enums import ChannelTypeFunctionName
from homeassistant.config_entries import ConfigEntry, ConfigEntryNotReady
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceEntry
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

from .aioenet import (
    URL,
    EnetClient,
    EnetConnectionError,
    EnetNoEventsRegistered,
    ActuatorChannel,
    SensorChannel,
)
from .const import (
    DOMAIN,
    ATTR_ENET_EVENT,
    EVENT_TYPE_INITIAL_PRESS,
    EVENT_TYPE_SHORT_RELEASE,
    EVENT_TYPE_LONG_RELEASE,
    NAME_ENET_CONTROLLER,
)
from .device import async_setup_devices

_LOGGER = logging.getLogger(__name__)
PLATFORMS: list[Platform] = [
    Platform.LIGHT,
    Platform.SCENE,
    Platform.COVER,
    Platform.SENSOR,
    Platform.SWITCH,
    Platform.BINARY_SENSOR,
]

EVENT_TYPE_CHANNELS = [
    ChannelTypeFunctionName.BUTTON_ROCKER,
    ChannelTypeFunctionName.MASTER_DIMMING,
    ChannelTypeFunctionName.SCENE_CONTROL,
    ChannelTypeFunctionName.TRIGGER_START,
]
EVENT_DEVICE_BATTERY_STATE_CHANGED = "deviceBatteryStateChanged"
EVENT_OUTPUT_DEVICE_FUNCTION_CALLED = "outputDeviceFunctionCalled"
EVENT_VALUE_TYPE_ROCKER_STATE = "VT_ROCKER_STATE"
EVENT_VALUE_TYPE_ROCKER_SWITCH_TIME = "VT_ROCKER_SWITCH_TIME"
EVENT_VALUE_DOWN_BUTTON = "DOWN_BUTTON"

PING_INTERVAL = 28  # seconds; keeps the HTTPS session alive
EVENT_RETRY_BASE_DELAY = 2  # seconds, multiplied by the number of failures
EVENT_RETRY_MAX_DELAY = 60  # seconds


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Enet Smart Home from a config entry."""
    _LOGGER.debug("Setting up Enet Smart Home entry")

    hass.data.setdefault(DOMAIN, {})
    hub = EnetClient(
        entry.data["url"],
        entry.data["username"],
        entry.data["password"],
        # noconnect=True,
        # load_file="/workspaces/enet-homeassistant/examples/config_entry-enet-oliver.json",
        # ,noconnect=True, load_file="/workspaces/enet-homeassistant/examples/config_entry-enet-01JRNS5MR3V9DX73M7BQ79KC40.json"
    )
    hub.coordinator = EnetCoordinator(hass, hub, entry)

    try:
        await hub.simple_login()
    except (asyncio.TimeoutError, aiohttp.ClientError, EnetConnectionError) as e:
        await hub.close()
        raise ConfigEntryNotReady("Failed to login to Enet Smart Home") from e

    hass.data[DOMAIN][entry.entry_id] = hub

    try:
        hub.devices = await hub.get_devices()
    except EnetConnectionError as e:
        hass.data[DOMAIN].pop(entry.entry_id)
        await hub.close()
        raise ConfigEntryNotReady("Enet Smart Home did not return its devices") from e
    except Exception as e:
        _LOGGER.error("Failed to get devices from Enet Smart Home: %s", e)
        hass.data[DOMAIN].pop(entry.entry_id)
        await hub.close()
        return False

    await async_setup_devices(hub.coordinator)
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    await hub.coordinator.setup_event_listeners()

    # Background loops are owned by the config entry, so Home Assistant cancels
    # them when the entry is unloaded or reloaded (no orphaned loops that keep
    # polling with an old session).
    hub.coordinator.start_background_tasks()
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    _LOGGER.debug("Unloading Enet Smart Home entry")
    if unload_ok := await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        hub = hass.data[DOMAIN].pop(entry.entry_id)
        hub.coordinator.stop_background_tasks()
        try:
            # Free the session on the server and close the HTTP session
            await hub.simple_logout()
        except Exception as e:  # pylint: disable=broad-except
            _LOGGER.debug("Logout from Enet server failed: %s", e)
            await hub.close()

    return unload_ok


async def async_remove_config_entry_device(
    hass: HomeAssistant, config_entry: ConfigEntry, device_entry: DeviceEntry
) -> bool:
    """Allow manual removal of devices the Enet server no longer reports.

    Devices that still exist on the Enet server (and the controller itself)
    cannot be removed, as they would be recreated on the next reload anyway.
    """
    hub = hass.data.get(DOMAIN, {}).get(config_entry.entry_id)
    if hub is None:
        return False

    known_uids = {device.uid for device in hub.devices}
    known_uids.add(NAME_ENET_CONTROLLER)

    return not any(
        identifier[0] == DOMAIN and identifier[1] in known_uids
        for identifier in device_entry.identifiers
    )


class EnetCoordinator(DataUpdateCoordinator):
    """Enet Smart Home coordinator responsible for subscribing to and handling events"""

    def __init__(
        self, hass: HomeAssistant, hub: EnetClient, entry: ConfigEntry
    ) -> None:
        """Initialize the coordinator."""
        self.hub = hub
        self.hass = hass
        self.config_entry = entry
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=None,
        )
        self._last_event: Dict[str, Any] = {}

        self.function_uid_map: Dict[str, Any] = {}
        self._background_tasks: list[asyncio.Task] = []
        _LOGGER.debug("EnetCoordinator initialized")

    def start_background_tasks(self) -> None:
        """Start the event loop (and the keep-alive ping for https)"""
        entry = self.config_entry
        self._background_tasks.append(
            entry.async_create_background_task(
                self.hass, self.async_refresh(), "enet event loop"
            )
        )
        if self.hub.baseurl.startswith("https://"):
            self._background_tasks.append(
                entry.async_create_background_task(
                    self.hass, self.ping_forever(), "enet keep-alive ping"
                )
            )

    def stop_background_tasks(self) -> None:
        """Cancel the event loop and ping before the session is closed"""
        for task in self._background_tasks:
            task.cancel()
        self._background_tasks.clear()

    async def setup_event_listeners(self) -> None:
        """Setup event listener for all output functions"""
        _LOGGER.debug("Setting up event listeners")
        await self.hub.setup_event_subscription_battery_state()
        for device in self.hub.devices:
            func_uids = device.get_function_uids_for_event()
            self.function_uid_map.update(func_uids)
            await device.register_events()

    async def resubscribe(self) -> None:
        """Log in again, register all events again and re-read the current
        values. Needed after the Enet server restarted: it then forgets the
        session and its event subscriptions."""
        await self.hub.simple_login()
        self.function_uid_map.clear()
        await self.setup_event_listeners()
        await self.refresh_current_values()
        _LOGGER.info("Re-subscribed to Enet server events")

    async def refresh_current_values(self) -> None:
        """Read the current value of every actuator output function, so states
        changed while events were not delivered are corrected"""
        for device in self.hub.devices:
            for channel in device.channels:
                if not isinstance(channel, ActuatorChannel):
                    continue
                for output_function in list(channel.output_functions.values()):
                    uid = output_function["uid"]
                    try:
                        result = await self.hub.request(
                            URL.VISUALIZATION,
                            "getCurrentValuesFromOutputDeviceFunction",
                            {"deviceFunctionUID": uid},
                        )
                    except Exception as e:  # pylint: disable=broad-except
                        _LOGGER.debug("Could not read %s (%s): %s", channel.name, uid, e)
                        continue
                    values = (result or {}).get("currentValues") or []
                    if len(values) == 1:
                        await channel.update_values(uid, values)
        self.async_update_listeners()

    async def ping_forever(self):
        """Ping server to keep conncetion alive"""
        while True:
            try:
                await self.hub.ping()
            except Exception as e:
                _LOGGER.warning(
                    "Failed to ping server: (%s), retrying in: %s", e, PING_INTERVAL
                )

            await asyncio.sleep(PING_INTERVAL)

    async def _async_update_data_offline(self) -> NoReturn:
        """Simulate events when offline by randomly generating events - only for debugging"""
        while True:
            await asyncio.sleep(10)
            if random.random() < 0.3:
                event = {
                    "sequenceNumber": 42,
                    "event": "outputDeviceFunctionCalled",
                    "eventData": {
                        "deviceUID": "1fb68d33-bc85-43aa-b459-34510bb08648",
                        "channelNumber": 1,
                        "deviceFunctionUID": "1fb68d33-bc85-43aa-b459-34510bb0866c",
                        "values": [
                            {
                                "value": 6.79,
                                "valueTypeID": "VT_VALUE_TIME1_RANGE_0.0_86400.0_DEF_0.0",
                            }
                        ],
                    },
                }
                await self.handle_event({"events": [event]})

    async def _async_update_data(self) -> NoReturn:
        """Fetch events from Enet server

        This endpoint blocks for 30s if no events are available
        or returns immediatly when events are there. Loop forever
        to get next events.

        This function and event processing should be moved to aioenet
        at some point.

        """
        if self.hub._offline:
            await self._async_update_data_offline()
            return

        failcount = 0
        while True:
            try:
                event = await self.hub.get_events()
                failcount = 0
            except EnetNoEventsRegistered:
                _LOGGER.warning(
                    "Enet server lost the event subscriptions (restarted?), "
                    "logging in and subscribing again"
                )
                try:
                    await self.resubscribe()
                    failcount = 0
                    continue
                except Exception as e:  # pylint: disable=broad-except
                    failcount += 1
                    delay = min(EVENT_RETRY_BASE_DELAY * failcount, EVENT_RETRY_MAX_DELAY)
                    _LOGGER.warning(
                        "Failed to subscribe again: (%s), retrying in: %s", e, delay
                    )
                    await asyncio.sleep(delay)
                    continue
            except Exception as e:
                failcount += 1
                delay = min(EVENT_RETRY_BASE_DELAY * failcount, EVENT_RETRY_MAX_DELAY)
                _LOGGER.warning(
                    "Failed to fetch events: (%s), retrying in: %s", e, delay
                )
                await asyncio.sleep(delay)
                continue
            if event:
                try:
                    await self.handle_event(event)
                except Exception as e:
                    _LOGGER.exception("Failed to handle event: %s (%s)", event, e)

    async def handle_event(self, event_data: Dict[str, Any]) -> None:
        """Handle events from Enet Server. Either update value of actuator or
        forward event from sensor
        """
        for event in event_data["events"]:
            _LOGGER.debug("Handling event: %s", event)
            if (
                time.time() - self._last_event.get("ts", 0) < 0.5
                and event["event"] == self._last_event.get("event")
                and event["eventData"] == self._last_event.get("eventData")
            ):
                # This is a duplicate of last event, happes with scene activitation
                _LOGGER.debug("Received duplicate event")
                continue

            self._last_event = event
            self._last_event["ts"] = time.time()

            if event["event"] == EVENT_OUTPUT_DEVICE_FUNCTION_CALLED:
                data = event["eventData"]
                function_uid = data["deviceFunctionUID"]
                device = self.function_uid_map.get(function_uid)
                if not device:
                    _LOGGER.warning("Function %s does not map to device", function_uid)
                    continue

                values = data["values"]
                channel_type = (
                    device.get_channel_type_function_name_from_output_function_uid(
                        function_uid
                    )
                )

                if channel_type in [
                    ChannelTypeFunctionName.BUTTON_ROCKER,
                    ChannelTypeFunctionName.SCENE_CONTROL,
                ]:
                    # Decode sensor / button events and forward to hass bus
                    subtype = data["channelNumber"]
                    if len(values) != 2:
                        _LOGGER.warning("Expected 2 values: %s", event)
                        continue

                    event_type = EVENT_TYPE_INITIAL_PRESS
                    if values[0]["valueTypeID"] == EVENT_VALUE_TYPE_ROCKER_STATE:
                        # If a button is configured as a rocker, you have the UP and Down
                        # button on the same channel.
                        if values[0]["value"] == EVENT_VALUE_DOWN_BUTTON:
                            subtype += 1
                    if values[1]["valueTypeID"] == EVENT_VALUE_TYPE_ROCKER_SWITCH_TIME:
                        # Switch time is 0 on press and a number for release.
                        # We don't distinguish between long and short press for the
                        # moment.
                        switch_time = values[1]["value"]
                        if switch_time > 0:
                            event_type = EVENT_TYPE_SHORT_RELEASE
                        if switch_time > 60:
                            event_type = EVENT_TYPE_LONG_RELEASE

                    bus_data = {
                        "device_id": device.device.hass_device_entry.id,
                        "unique_id": device.device.uid,
                        "type": event_type,
                        "subtype": str(subtype),
                    }
                    self.hass.bus.async_fire(ATTR_ENET_EVENT, bus_data)
                else:
                    await device.update_values(function_uid, values)

            elif event["event"] == EVENT_DEVICE_BATTERY_STATE_CHANGED:
                # _LOGGER.debug("Battery state changed: %s", event["eventData"])
                data = event["eventData"]
                device_uid = data.get("deviceUID", None)
                battery_state = data.get("batteryState", None)
                device = next(
                    (d for d in self.hub.devices if d.uid == device_uid), None
                )
                if device:
                    device.update_battery_state(battery_state)
                    self.async_update_listeners()
