'use strict';

const utils = require('@iobroker/adapter-core');
const adapterName = require('./package.json').name.split('.').pop();
const schedule = require('node-schedule');
const arrApart = require('./lib/arrApart.js'); // list of supported adapters
const translations = require('./lib/translations.js');
const cronParser = require('cron-parser');

// indicator if the adapter is running or not (for intervall/shedule)
let isUnloaded = false;

class DeviceWatcher extends utils.Adapter {
	constructor(options) {
		super({
			...options,
			name: adapterName,
			useFormatDate: true,
		});

		// instances and adapters
		// raw arrays
		this.listInstanceRaw = new Map();
		this.adapterUpdatesJsonRaw = new Map();
		this.listErrorInstanceRaw = [];

		// user arrays
		this.listAllInstances = [];
		this.listAllActiveInstances = [];
		this.listDeactivatedInstances = [];
		this.listAdapterUpdates = [];
		this.listErrorInstance = [];

		//counts
		this.countAllInstances = 0;
		this.countAllActiveInstances = 0;
		this.countDeactivatedInstances = 0;
		this.countAdapterUpdates = 0;
		this.countErrorInstance = 0;

		// devices
		// raw arrays
		this.listAllDevicesRaw = new Map();
		this.batteryLowPoweredRaw = [];
		this.offlineDevicesRaw = [];
		this.upgradableDevicesRaw = [];

		// arrays
		this.listAllDevicesUserRaw = [];
		this.listAllDevices = [];
		this.offlineDevices = [];
		this.linkQualityDevices = [];
		this.batteryPowered = [];
		this.batteryLowPowered = [];
		this.selAdapter = [];
		this.adapterSelected = [];
		this.upgradableList = [];

		// counts
		this.offlineDevicesCount = 0;
		this.deviceCounter = 0;
		this.linkQualityCount = 0;
		this.batteryPoweredCount = 0;
		this.lowBatteryPoweredCount = 0;
		this.upgradableDevicesCount = 0;

		// Blacklist
		// Instances
		this.blacklistInstancesLists = [];
		this.blacklistInstancesNotify = [];

		// Devices
		this.blacklistLists = [];
		this.blacklistAdapterLists = [];
		this.blacklistNotify = [];

		// Timelist instances
		this.userTimeInstancesList = new Map();

		// Interval timer
		this.refreshDataTimeout = null;

		// Check if main function is running
		this.mainRunning = false;

		// System anguage
		this.userSystemLanguage;

		this.on('ready', this.onReady.bind(this));
		this.on('stateChange', this.onStateChange.bind(this));
		this.on('objectChange', this.onObjectChange.bind(this));
		this.on('message', this.onMessage.bind(this));
		this.on('unload', this.onUnload.bind(this));
	}

	/**
	 * onReady
	 */
	async onReady() {
		this.log.debug(`Adapter ${adapterName} was started`);

		// set user language
		if (this.userSystemLanguage === undefined && this.userSystemLanguage === null) {
			this.userSystemLanguage = 'de';
		} else {
			this.userSystemLanguage = this.language;
		}

		this.configCreateInstanceList = this.config.checkAdapterInstances;
		this.configListOnlyBattery = this.config.listOnlyBattery;
		this.configCreateOwnFolder = this.config.createOwnFolder;
		this.configCreateHtmlList = this.config.createHtmlList;

		this.configSetAdapter = {
			alexa2: this.config.alexa2Devices,
			apcups: this.config.apcupsDevices,
			ble: this.config.bleDevices,
			deconz: this.config.deconzDevices,
			ecovacsdeebot: this.config.ecovacsdeebotDevices,
			enocean: this.config.enoceanDevices,
			esphome: this.config.esphomeDevices,
			eusec: this.config.eusecDevices,
			fhemTFAsensors: this.config.fhemTFAsensorsDevices,
			fritzdect: this.config.fritzdectDevices,
			fullybrowser: this.config.fullybrowserDevices,
			fullybrowserV3: this.config.fullybrowserV3Devices,
			fullyMQTT: this.config.fullyMQTTDevices,
			ham: this.config.hamDevices,
			harmony: this.config.harmonyDevices,
			hmiP: this.config.hmiPDevices,
			hmrpc: this.config.hmrpcDevices,
			homeconnect: this.config.homeconnectDevices,
			homekitController: this.config.homekitControllerDevices,
			hs100: this.config.hs100Devices,
			hue: this.config.hueDevices,
			hueExt: this.config.hueExtDevices,
			innogy: this.config.innogyDevices,
			jeelink: this.config.jeelinkDevices,
			loqedSmartLock: this.config.loqedSmartLockDevices,
			lupusec: this.config.lupusecDevices,
			maxcube: this.config.maxcubeDevices,
			meross: this.config.merossDevices,
			mihome: this.config.mihomeDevices,
			mihomeGW: this.config.mihomeDevices,
			mihomeVacuum: this.config.mihomeVacuumDevices,
			mqttClientZigbee2Mqtt: this.config.mqttClientZigbee2MqttDevices,
			mqttNuki: this.config.mqttNukiDevices,
			musiccast: this.config.musiccastDevices,
			netatmo: this.config.netatmoDevices,
			nukiExt: this.config.nukiExtDevices,
			nut: this.config.nutDevices,
			ping: this.config.pingDevices,
			proxmox: this.config.proxmoxDevices,
			roomba: this.config.roombaDevices,
			shelly: this.config.shellyDevices,
			smartgarden: this.config.smartgardenDevices,
			sonoff: this.config.sonoffDevices,
			sonos: this.config.sonosDevices,
			sureflap: this.config.sureflapDevices,
			switchbotBle: this.config.switchbotBleDevices,
			tado: this.config.tadoDevices,
			tapo: this.config.tapoDevices,
			tradfri: this.config.tradfriDevices,
			tuya: this.config.tuyaDevices,
			unifi: this.config.unifiDevices,
			viessmann: this.config.viessmannDevices,
			wifilight: this.config.wifilightDevices,
			wled: this.config.wledDevices,
			yeelight: this.config.yeelightDevices,
			zigbee: this.config.zigbeeDevices,
			zigbee2MQTT: this.config.zigbee2mqttDevices,
			zwave2: this.config.zwaveDevices,
		};

		this.configMaxMinutes = {
			alexa2: this.config.alexa2MaxMinutes,
			apcups: this.config.apcupsMaxMinutes,
			ble: this.config.bleMaxMinutes,
			deconz: this.config.deconzMaxMinutes,
			ecovacsdeebot: this.config.ecovacsdeebotMaxMinutes,
			enocean: this.config.enoceanMaxMinutes,
			esphome: this.config.esphomeMaxMinutes,
			eusec: this.config.eusecMaxMinutes,
			fhemTFAsensors: this.config.fhemTFAsensorsMaxMinutes,
			fritzdect: this.config.fritzdectMaxMinutes,
			fullybrowser: this.config.fullybrowserMaxMinutes,
			fullybrowserV3: this.config.fullybrowserV3MaxMinutes,
			fullyMQTT: this.config.fullyMQTTMaxMinutes,
			ham: this.config.hamMaxMinutes,
			harmony: this.config.harmonyMaxMinutes,
			hmiP: this.config.hmiPMaxMinutes,
			hmrpc: this.config.hmrpcMaxMinutes,
			homeconnect: this.config.homeconnectMaxMinutes,
			homekitController: this.config.homekitControllerMaxMinutes,
			hs100: this.config.hs100MaxMinutes,
			hue: this.config.hueMaxMinutes,
			hueExt: this.config.hueextMaxMinutes,
			innogy: this.config.innogyMaxMinutes,
			jeelink: this.config.jeelinkMaxMinutes,
			loqedSmartLock: this.config.loqedSmartLockMaxMinutes,
			lupusec: this.config.lupusecMaxMinutes,
			maxcube: this.config.maxcubeMaxMinutes,
			meross: this.config.merossMaxMinutes,
			mihome: this.config.mihomeMaxMinutes,
			mihomeGW: this.config.mihomeMaxMinutes,
			mihomeVacuum: this.config.mihomeVacuumMaxMinutes,
			mqttClientZigbee2Mqtt: this.config.mqttClientZigbee2MqttMaxMinutes,
			mqttNuki: this.config.mqttNukiMaxMinutes,
			musiccast: this.config.musiccastMaxMinutes,
			netatmo: this.config.netatmoMaxMinutes,
			nukiExt: this.config.nukiextendMaxMinutes,
			nut: this.config.nutMaxMinutes,
			ping: this.config.pingMaxMinutes,
			proxmox: this.config.proxmoxMaxMinutes,
			roomba: this.config.roombaMaxMinutes,
			shelly: this.config.shellyMaxMinutes,
			smartgarden: this.config.smartgardenMaxMinutes,
			sonoff: this.config.sonoffMaxMinutes,
			sonos: this.config.sonosMaxMinutes,
			sureflap: this.config.sureflapMaxMinutes,
			switchbotBle: this.config.switchbotMaxMinutes,
			tado: this.config.tadoMaxMinutes,
			tapo: this.config.tapoMaxMinutes,
			tradfri: this.config.tradfriMaxMinutes,
			tuya: this.config.tuyaMaxMinutes,
			unifi: this.config.unifiMaxMinutes,
			viessmann: this.config.viessmannMaxMinutes,
			wifilight: this.config.wifilightMaxMinutes,
			wled: this.config.wledMaxMinutes,
			yeelight: this.config.yeelightMaxMinutes,
			zigbee: this.config.zigbeeMaxMinutes,
			zigbee2MQTT: this.config.zigbee2mqttMaxMinutes,
			zwave2: this.config.zwaveMaxMinutes,
		};

		try {
			// create list with selected adapters for monitor devices
			for (const [id] of Object.entries(arrApart)) {
				if (this.configSetAdapter[id]) {
					this.selAdapter.push(arrApart[id]);
					this.adapterSelected.push(this.capitalize(id));
				}
			}

			// Check if an adapter to monitor devices is selected.
			if (this.adapterSelected.length >= 1) {
				// show list in debug log
				this.log.debug(JSON.stringify(this.selAdapter));

				this.log.info(`Number of selected adapters to monitor devices: ${this.adapterSelected.length}. Loading data from: ${this.adapterSelected.join(', ')} ...`);
			} else {
				this.log.info(`No adapters selected to monitor devices.`);
			}

			// create Blacklist
			await this.createBlacklist();

			// create user defined list with time of error for instances
			await this.createTimeListInstances();

			//create datapoints for each adapter if selected
			for (const [id] of Object.entries(arrApart)) {
				try {
					if (!this.configCreateOwnFolder) {
						await this.deleteDPsForEachAdapter(id);
						await this.deleteHtmlListDatapoints(id);
					} else {
						if (this.configSetAdapter && this.configSetAdapter[id]) {
							await this.createDPsForEachAdapter(id);
							// create HTML list datapoints
							if (!this.configCreateHtmlList) {
								await this.deleteHtmlListDatapoints(id);
							} else {
								await this.createHtmlListDatapoints(id);
							}
							this.log.debug(`Created datapoints for ${this.capitalize(id)}`);
						}
					}
				} catch (error) {
					this.log.error(`[onReady - create and fill datapoints for each adapter] - ${error}`);
				}
			}

			// create HTML list datapoints
			if (!this.configCreateHtmlList) {
				await this.deleteHtmlListDatapoints();
				await this.deleteHtmlListDatapointsInstances();
			} else {
				await this.createHtmlListDatapoints();
				if (this.config.checkAdapterInstances) await this.createHtmlListDatapointsInstances();
			}
			if (!this.config.checkAdapterInstances) await this.deleteHtmlListDatapointsInstances();

			// read data first at start
			// devices
			await this.main();

			// instances and adapters
			if (this.configCreateInstanceList) {
				// instances
				await this.createDPsForInstances();
				await this.getInstanceData(`system.adapter.*`);
				// adapter updates
				await this.createAdapterUpdateData();
			} else {
				await this.deleteDPsForInstances();
			}

			// update last contact data in interval
			await this.refreshData();

			// send overview for low battery devices
			if (this.config.checkSendBatteryMsgDaily) await this.sendScheduleNotifications('lowBatteryDevices');

			// send overview of offline devices
			if (this.config.checkSendOfflineMsgDaily) await this.sendScheduleNotifications('offlineDevices');

			// send overview of upgradeable devices
			if (this.config.checkSendUpgradeMsgDaily) await this.sendScheduleNotifications('updateDevices');

			// send overview of updatable adapters
			if (this.config.checkSendAdapterUpdateMsgDaily) await this.sendScheduleNotifications('updateAdapter');

			// send overview of deactivated instances
			if (this.config.checkSendInstanceDeactivatedDaily) await this.sendScheduleNotifications('deactivatedInstance');

			// send overview of instances with error
			if (this.config.checkSendInstanceFailedDaily) await this.sendScheduleNotifications('errorInstance');
		} catch (error) {
			this.log.error(`[onReady] - ${error}`);
			this.terminate ? this.terminate(15) : process.exit(15);
		}
	} // <-- onReady end

	// If you need to react to object changes, uncomment the following block and the corresponding line in the constructor.
	// You also need to subscribe to the objects with `this.subscribeObjects`, similar to `this.subscribeStates`.
	//
	/**
	 * Is called if a subscribed object changes
	 * @param {string} id
	 * @param {ioBroker.Object | null | undefined} obj
	 */
	async onObjectChange(id, obj) {
		if (obj) {
			try {
				// The object was changed
				//this.log.debug(`object ${id} changed: ${JSON.stringify(obj)}`);

				if (this.config.checkAdapterInstances && id.startsWith('system.adapter.')) {
					//read new instance data and add it to the lists
					await this.getInstanceData(id);
				} else {
					if (Array.from(this.listAllDevicesRaw.values()).some((obj) => obj.mainSelector === id)) {
						if (!this.mainRunning) {
							await this.main();
						} else {
							return;
						}
					} else {
						return;
					}
				}
			} catch (error) {
				this.log.error(`Issue at object change: ${error}`);
			}
		} else {
			try {
				// The object was deleted
				this.log.debug(`object ${id} deleted`);

				// delete instance data in map
				if (this.listInstanceRaw.has(id)) {
					this.listInstanceRaw.delete(id);
				}

				// delete device data in map
				if (this.listAllDevicesRaw.has(id)) {
					this.listAllDevicesRaw.delete(id);
				}

				//unsubscribe of Objects and states
				this.unsubscribeForeignObjects(id);
				this.unsubscribeForeignStates(id);
			} catch (error) {
				this.log.error(`Issue at object deletion: ${error}`);
			}
		}
	}

	/**
	 * Is called if a subscribed state changes
	 * @param {string} id
	 * @param {ioBroker.State | null | undefined} state
	 */
	async onStateChange(id, state) {
		if (state) {
			this.log.debug(`State changed: ${id} changed ${state.val}`);

			try {
				/*=============================================
				=        	Instances / Adapter     	     =
				=============================================*/
				if (this.config.checkAdapterInstances) {
					// Adapter Update data
					if (id.endsWith('updatesJson')) {
						await this.renewAdapterUpdateData(id);
					}
					// Instanz data
					if (Array.from(this.listInstanceRaw.values()).some((obj) => Object.values(obj).includes(id))) {
						await this.renewInstanceData(id, state);
					}
				}

				/*=============================================
				=          		  Devices     			      =
				=============================================*/
				if (Array.from(this.listAllDevicesRaw.values()).some((obj) => Object.values(obj).includes(id))) {
					await this.renewDeviceData(id, state);
				}
			} catch (error) {
				this.log.error(`Issue at state change: ${error}`);
			}
		} else {
			// The state was deleted
			this.log.debug(`state ${id} deleted`);
		}
	}

	/**
	 * @param {ioBroker.Message} obj
	 */
	onMessage(obj) {
		const devices = [];
		const instances = [];
		const instancesTime = [];
		let countDevices = 0;
		let countInstances = 0;

		switch (obj.command) {
			case 'devicesList':
				if (obj.message) {
					try {
						for (const deviceData of this.listAllDevicesRaw.values()) {
							const label = `${deviceData.Adapter}: ${deviceData.Device}`;
							const valueObjectDevices = {
								deviceName: deviceData.Device,
								adapter: deviceData.Adapter,
								path: deviceData.Path,
							};
							devices[countDevices] = { label: label, value: JSON.stringify(valueObjectDevices) };
							countDevices++;
						}
						const sortDevices = devices.slice(0);
						sortDevices.sort(function (a, b) {
							const x = a.label;
							const y = b.label;
							return x < y ? -1 : x > y ? 1 : 0;
						});
						this.sendTo(obj.from, obj.command, sortDevices, obj.callback);
					} catch (error) {
						this.log.error(`[onMessage - deviceList for blacklisttable] - ${error}`);
					}
				}
				break;

			case 'instancesList':
				if (obj.message) {
					try {
						for (const [instance, instanceData] of this.listInstanceRaw) {
							const label = `${instanceData.Adapter}: ${instance}`;
							const valueObjectInstances = {
								adapter: instanceData.Adapter,
								instanceID: instance,
							};
							instances[countInstances] = { label: label, value: JSON.stringify(valueObjectInstances) };
							countInstances++;
						}
						const sortInstances = instances.slice(0);
						sortInstances.sort(function (a, b) {
							const x = a.label;
							const y = b.label;
							return x < y ? -1 : x > y ? 1 : 0;
						});
						this.sendTo(obj.from, obj.command, sortInstances, obj.callback);
					} catch (error) {
						this.log.error(`[onMessage - instanceList] - ${error}`);
					}
				}
				break;
			case 'instancesListTime':
				if (obj.message) {
					try {
						for (const [instance, instanceData] of this.listInstanceRaw) {
							const label = `${instanceData.Adapter}: ${instance}`;
							const valueObjectInstances = {
								adapter: instanceData.Adapter,
								instanceName: instance,
							};
							instancesTime[countInstances] = { label: label, value: JSON.stringify(valueObjectInstances) };
							countInstances++;
						}
						const sortInstances = instancesTime.slice(0);
						sortInstances.sort(function (a, b) {
							const x = a.label;
							const y = b.label;
							return x < y ? -1 : x > y ? 1 : 0;
						});
						this.sendTo(obj.from, obj.command, sortInstances, obj.callback);
					} catch (error) {
						this.log.error(`[onMessage - instanceList] - ${error}`);
					}
				}
				break;
		}
	}

	/**
	 * main function
	 */
	async main() {
		this.log.debug(`Function started: ${this.main.name}`);
		this.mainRunning = true;

		// cancel run if no adapter is selected
		if (this.adapterSelected.length === 0) return;

		// fill counts and lists of all selected adapter
		try {
			for (let i = 0; i < this.selAdapter.length; i++) {
				await this.createData(i);
				await this.createLists();
			}
			await this.writeDatapoints(); // fill the datapoints
			this.log.debug(`Created and filled data for all adapters`);
		} catch (error) {
			this.log.error(`[main - create data of all adapter] - ${error}`);
		}

		// fill datapoints for each adapter if selected
		if (this.configCreateOwnFolder) {
			try {
				for (const [id] of Object.entries(arrApart)) {
					if (this.configSetAdapter && this.configSetAdapter[id]) {
						for (const deviceData of this.listAllDevicesRaw.values()) {
							// list device only if selected adapter matched with device
							if (!deviceData.adapterID.includes(id)) continue;
							await this.createLists(id);
						}
						await this.writeDatapoints(id); // fill the datapoints
						this.log.debug(`Created and filled data for ${this.capitalize(id)}`);
					}
				}
			} catch (error) {
				this.log.error(`[main - create and fill datapoints for each adapter] - ${error}`);
			}
		}
		this.mainRunning = false;
		this.log.debug(`Function finished: ${this.main.name}`);
	} //<--End of main function

	/**
	 * refresh data with interval
	 * is neccessary to refresh lastContact data, especially of devices without state changes
	 */
	async refreshData() {
		if (isUnloaded) return; // cancel run if unloaded was called.
		const nextTimeout = this.config.updateinterval * 1000;

		// devices data
		await this.checkLastContact();
		await this.createLists();
		await this.writeDatapoints();

		// devices data in own adapter folder
		if (this.configCreateOwnFolder) {
			for (const [id] of Object.entries(arrApart)) {
				if (this.configSetAdapter && this.configSetAdapter[id]) {
					await this.createLists(id);
					await this.writeDatapoints(id);
					this.log.debug(`Created and filled data for ${this.capitalize(id)}`);
				}
			}
		}

		// instance and adapter data
		if (this.configCreateInstanceList) {
			await this.createInstanceList();
			await this.writeInstanceDPs();
		}

		// Clear existing timeout
		if (this.refreshDataTimeout) {
			this.clearTimeout(this.refreshDataTimeout);
			this.refreshDataTimeout = null;
		}

		this.refreshDataTimeout = this.setTimeout(async () => {
			this.log.debug('Updating Data');
			await this.refreshData();
		}, nextTimeout);
	} // <-- refreshData end

	/**
	 * create blacklist
	 */
	async createBlacklist() {
		this.log.debug(`Function started: ${this.createBlacklist.name}`);

		// DEVICES
		const myBlacklist = this.config.tableBlacklist;
		if (myBlacklist.length >= 1) {
			for (const i in myBlacklist) {
				try {
					const blacklistParse = this.parseData(myBlacklist[i].devices);
					// push devices in list to ignor device in lists
					if (myBlacklist[i].checkIgnorLists) {
						this.blacklistLists.push(blacklistParse.path);
					}
					if (myBlacklist[i].checkIgnorAdapterLists) {
						this.blacklistAdapterLists.push(blacklistParse.path);
					}
					// push devices in list to ignor device in notifications
					if (myBlacklist[i].checkIgnorNotify) {
						this.blacklistNotify.push(blacklistParse.path);
					}
				} catch (error) {
					this.log.error(`[createBlacklist] - ${error}`);
				}
				if (this.blacklistLists.length >= 1) this.log.info(`Found devices/services on blacklist for lists: ${this.blacklistLists}`);
				if (this.blacklistAdapterLists.length >= 1) this.log.info(`Found devices/services on blacklist for own adapter lists: ${this.blacklistAdapterLists}`);
				if (this.blacklistNotify.length >= 1) this.log.info(`Found devices/services on blacklist for notifications: ${this.blacklistNotify}`);
			}
		}

		// INSTANCES
		const myBlacklistInstances = this.config.tableBlacklistInstances;
		if (myBlacklistInstances.length >= 1) {
			for (const i in myBlacklistInstances) {
				try {
					const blacklistParse = this.parseData(myBlacklistInstances[i].instances);
					// push devices in list to ignor device in lists
					if (myBlacklistInstances[i].checkIgnorLists) {
						this.blacklistInstancesLists.push(blacklistParse.instanceID);
					}
					// push devices in list to ignor device in notifications
					if (myBlacklistInstances[i].checkIgnorNotify) {
						this.blacklistInstancesNotify.push(blacklistParse.instanceID);
					}
				} catch (error) {
					this.log.error(`[createBlacklist] - ${error}`);
				}
			}
			if (this.blacklistInstancesLists.length >= 1) this.log.info(`Found instances items on blacklist for lists: ${this.blacklistInstancesLists}`);
			if (this.blacklistInstancesNotify.length >= 1) this.log.info(`Found instances items on blacklist for notifications: ${this.blacklistInstancesNotify}`);
		}
		this.log.debug(`Function finished: ${this.createBlacklist.name}`);
	}

	/**
	 * create list with time for instances
	 */
	async createTimeListInstances() {
		// INSTANCES
		const userTimeListInstances = this.config.tableTimeInstance;
		if (userTimeListInstances.length >= 1) {
			for (const i in userTimeListInstances) {
				try {
					const userTimeListparse = this.parseData(userTimeListInstances[i].instancesTime);
					// push devices in list to ignor device in lists
					this.userTimeInstancesList.set(userTimeListparse.instanceName, {
						deactivationTime: userTimeListInstances[i].deactivationTime,
						errorTime: userTimeListInstances[i].errorTime,
					});
				} catch (error) {
					this.log.error(`[createTimeListInstances] - ${error}`);
				}
			}
			if (this.userTimeInstancesList.size >= 1) this.log.info(`Found instances items on lists for timesettings: ${Array.from(this.userTimeInstancesList.keys())}`);
		}
	}

	/**
	 * @param {object} i - Device Object
	 */
	async createData(i) {
		try {
			const devices = await this.getForeignStatesAsync(this.selAdapter[i].Selektor);
			const adapterID = this.selAdapter[i].adapterID;

			/*----------  Start of loop  ----------*/
			for (const [id] of Object.entries(devices)) {
				if (id.endsWith('.')) continue;
				const mainSelector = id;

				/*=============================================
				=              get Instanz		          =
				=============================================*/
				const instance = id.slice(0, id.indexOf('.') + 2);

				const instanceDeviceConnectionDP = `${instance}.info.connection`;
				const instancedeviceConnected = await this.getInitValue(instanceDeviceConnectionDP);
				this.subscribeForeignStates(instanceDeviceConnectionDP);
				this.subscribeForeignObjects(`${this.selAdapter[i].Selektor}`);
				// this.subscribeForeignObjects('*');
				//this.subscribeForeignStates('*');
				/*=============================================
				=              Get device name		          =
				=============================================*/
				const deviceName = await this.getDeviceName(id, i);

				/*=============================================
				=              Get adapter name		          =
				=============================================*/
				const adapter = this.selAdapter[i].adapter;

				/*=============================================
				=            Get path to datapoints	   	      =
				=============================================*/
				const currDeviceString = id.slice(0, id.lastIndexOf('.') + 1 - 1);
				const shortCurrDeviceString = currDeviceString.slice(0, currDeviceString.lastIndexOf('.') + 1 - 1);

				// subscribe to object device path
				this.subscribeForeignStates(currDeviceString);

				/*=============================================
				=            Get signal strength              =
				=============================================*/
				let deviceQualityDP = currDeviceString + this.selAdapter[i].rssiState;
				let deviceQualityState;

				switch (adapterID) {
					case 'mihomeVacuum':
						deviceQualityDP = shortCurrDeviceString + this.selAdapter[i].rssiState;
						deviceQualityState = await this.getForeignStateAsync(deviceQualityDP);
						break;

					case 'netatmo':
						deviceQualityState = await this.getForeignStateAsync(deviceQualityDP);
						if (!deviceQualityState) {
							deviceQualityDP = currDeviceString + this.selAdapter[i].rfState;
							deviceQualityState = await this.getForeignStateAsync(deviceQualityDP);
						}
						break;

					default:
						deviceQualityState = await this.getForeignStateAsync(deviceQualityDP);
						break;
				}
				//subscribe to states
				this.subscribeForeignStates(deviceQualityDP);

				const signalData = await this.calculateSignalStrength(deviceQualityState, adapterID);
				let linkQuality = signalData[0];
				const linkQualityRaw = signalData[1];

				/*=============================================
				=         	    Get battery data       	      =
				=============================================*/
				let deviceBatteryStateDP;
				let deviceBatteryState;
				let batteryHealth;
				let batteryHealthRaw;
				let batteryUnitRaw;
				let lowBatIndicator;
				let isBatteryDevice;
				let isLowBatDP;
				let faultReportingDP;
				let faultReportingState;

				const deviceChargerStateDP = currDeviceString + this.selAdapter[i].charger;
				const deviceChargerState = await this.getInitValue(deviceChargerStateDP);

				if (deviceChargerState === undefined || deviceChargerState === false) {
					// Get battery states
					switch (adapterID) {
						case 'hmrpc':
							deviceBatteryStateDP = currDeviceString + this.selAdapter[i].battery;
							deviceBatteryState = await this.getInitValue(deviceBatteryStateDP);

							if (deviceBatteryState === undefined) {
								deviceBatteryStateDP = shortCurrDeviceString + this.selAdapter[i].hmDNBattery;
								deviceBatteryState = await this.getInitValue(deviceBatteryStateDP);
							}
							break;
						case 'hueExt':
						case 'mihomeVacuum':
						case 'mqttNuki':
						case 'loqedSmartLock':
							deviceBatteryStateDP = shortCurrDeviceString + this.selAdapter[i].battery;
							deviceBatteryState = await this.getInitValue(deviceBatteryStateDP);

							if (deviceBatteryState === undefined) {
								deviceBatteryStateDP = shortCurrDeviceString + this.selAdapter[i].battery2;
								deviceBatteryState = await this.getInitValue(deviceBatteryStateDP);
							}
							break;
						default:
							deviceBatteryStateDP = currDeviceString + this.selAdapter[i].battery;
							deviceBatteryState = await this.getInitValue(deviceBatteryStateDP);

							if (deviceBatteryState === undefined) {
								deviceBatteryStateDP = currDeviceString + this.selAdapter[i].battery2;
								deviceBatteryState = await this.getInitValue(deviceBatteryStateDP);

								if (deviceBatteryState === undefined) {
									deviceBatteryStateDP = currDeviceString + this.selAdapter[i].battery3;
									deviceBatteryState = await this.getInitValue(deviceBatteryStateDP);
								}
							}
							break;
					}

					// Get low bat states
					isLowBatDP = currDeviceString + this.selAdapter[i].isLowBat;
					let deviceLowBatState = await this.getInitValue(isLowBatDP);

					if (deviceLowBatState === undefined) {
						isLowBatDP = currDeviceString + this.selAdapter[i].isLowBat2;
						deviceLowBatState = await this.getInitValue(isLowBatDP);

						if (deviceLowBatState === undefined) {
							isLowBatDP = currDeviceString + this.selAdapter[i].isLowBat3;
							deviceLowBatState = await this.getInitValue(isLowBatDP);
						}
					}
					if (deviceLowBatState === undefined) isLowBatDP = 'none';

					faultReportingDP = shortCurrDeviceString + this.selAdapter[i].faultReporting;
					faultReportingState = await this.getInitValue(faultReportingDP);

					//subscribe to states
					this.subscribeForeignStates(deviceBatteryStateDP);
					this.subscribeForeignStates(isLowBatDP);
					this.subscribeForeignStates(faultReportingDP);

					const batteryData = await this.getBatteryData(deviceBatteryState, deviceLowBatState, faultReportingState, adapterID);
					batteryHealth = batteryData[0];
					batteryHealthRaw = batteryData[2];
					batteryUnitRaw = batteryData[3];
					isBatteryDevice = batteryData[1];

					if (isBatteryDevice) {
						lowBatIndicator = await this.setLowbatIndicator(deviceBatteryState, deviceLowBatState, faultReportingState, adapterID);
					}
				}

				/*=============================================
				=          Get last contact of device         =
				=============================================*/
				let unreachDP = currDeviceString + this.selAdapter[i].reach;
				const deviceStateSelectorDP = shortCurrDeviceString + this.selAdapter[i].stateValue;
				const rssiPeerSelectorDP = currDeviceString + this.selAdapter[i].rssiPeerState;
				let timeSelector = currDeviceString + this.selAdapter[i].timeSelector;

				const timeSelectorState = await this.getInitValue(timeSelector);
				if (timeSelectorState === undefined) {
					timeSelector = shortCurrDeviceString + this.selAdapter[i].timeSelector;
				}

				let deviceUnreachState = await this.getInitValue(unreachDP);
				if (deviceUnreachState === undefined) {
					unreachDP = shortCurrDeviceString + this.selAdapter[i].reach;
					deviceUnreachState = await this.getInitValue(shortCurrDeviceString + this.selAdapter[i].reach);
				}

				// subscribe to states
				this.subscribeForeignStates(timeSelector);
				this.subscribeForeignStates(unreachDP);
				this.subscribeForeignStates(deviceStateSelectorDP);
				this.subscribeForeignStates(rssiPeerSelectorDP);

				const onlineState = await this.getOnlineState(timeSelector, adapterID, unreachDP, linkQuality, deviceUnreachState, deviceStateSelectorDP, rssiPeerSelectorDP);
				let deviceState;
				let lastContactString;

				if (onlineState) {
					lastContactString = onlineState[0];
					deviceState = onlineState[1];
					linkQuality = onlineState[2];
				}

				/*=============================================
			=            Get update data	              =
			=============================================*/
				let isUpgradable;
				let deviceUpdateDP;

				if (this.config.checkSendDeviceUpgrade) {
					deviceUpdateDP = currDeviceString + this.selAdapter[i].upgrade;
					let deviceUpdateSelector = await this.getInitValue(deviceUpdateDP);
					if (deviceUpdateSelector === undefined) {
						deviceUpdateDP = shortCurrDeviceString + this.selAdapter[i].upgrade;
						deviceUpdateSelector = await this.getInitValue(deviceUpdateDP);
						if (deviceUpdateSelector === undefined) {
							const shortShortCurrDeviceString = shortCurrDeviceString.slice(0, shortCurrDeviceString.lastIndexOf('.') + 1 - 1);
							deviceUpdateDP = shortShortCurrDeviceString + this.selAdapter[i].upgrade;
							deviceUpdateSelector = await this.getInitValue(deviceUpdateDP);
						}
					}

					if (deviceUpdateSelector !== undefined) {
						isUpgradable = await this.checkDeviceUpdate(adapterID, deviceUpdateSelector);
					} else {
						isUpgradable = ' - ';
					}

					// subscribe to states
					this.subscribeForeignStates(deviceUpdateDP);
					// this.subscribeForeignStates('*');
				}

				/*=============================================
				=          		  Fill Raw Lists          	  =
				=============================================*/
				const setupList = () => {
					this.listAllDevicesRaw.set(currDeviceString, {
						Path: id,
						mainSelector: mainSelector,
						instanceDeviceConnectionDP: instanceDeviceConnectionDP,
						instancedeviceConnected: instancedeviceConnected,
						instance: instance,
						Device: deviceName,
						adapterID: adapterID,
						Adapter: adapter,
						timeSelector: timeSelector,
						isBatteryDevice: isBatteryDevice,
						Battery: batteryHealth,
						BatteryRaw: batteryHealthRaw,
						BatteryUnitRaw: batteryUnitRaw,
						batteryDP: deviceBatteryStateDP,
						LowBat: lowBatIndicator,
						LowBatDP: isLowBatDP,
						faultReport: faultReportingState,
						faultReportDP: faultReportingDP,
						SignalStrengthDP: deviceQualityDP,
						SignalStrength: linkQuality,
						SignalStrengthRaw: linkQualityRaw,
						UnreachState: deviceUnreachState,
						UnreachDP: unreachDP,
						DeviceStateSelectorDP: deviceStateSelectorDP,
						rssiPeerSelectorDP: rssiPeerSelectorDP,
						LastContact: lastContactString,
						Status: deviceState,
						UpdateDP: deviceUpdateDP,
						Upgradable: isUpgradable,
					});
				};

				if (!this.configListOnlyBattery) {
					// Add all devices
					setupList();
				} else {
					// Add only devices with battery in the rawlist
					if (!isBatteryDevice) continue;
					setupList();
				}
			} // <-- end of loop
		} catch (error) {
			this.log.error(`[createData - create data of devices] - ${error}`);
		}
	} // <-- end of createData

	/*=============================================
	=            functions to get data            =
	=============================================*/

	/**
	 * @param {object} id - deviceID
	 * @param {object} i - each Device
	 */
	async getDeviceName(id, i) {
		try {
			//id = id.replace(/[\]\\[.*,;'"`<>\\\s?]/g, '-');

			const currDeviceString = id.slice(0, id.lastIndexOf('.') + 1 - 1);
			const shortCurrDeviceString = currDeviceString.slice(0, currDeviceString.lastIndexOf('.') + 1 - 1);
			const shortshortCurrDeviceString = shortCurrDeviceString.slice(0, shortCurrDeviceString.lastIndexOf('.') + 1 - 1);

			// Get device name
			const deviceObject = await this.getForeignObjectAsync(currDeviceString);
			const shortDeviceObject = await this.getForeignObjectAsync(shortCurrDeviceString);
			const shortshortDeviceObject = await this.getForeignObjectAsync(shortshortCurrDeviceString);
			let deviceName;
			let folderName;
			let deviceID;

			switch (this.selAdapter[i].adapterID) {
				case 'fullybrowser':
					deviceName = (await this.getInitValue(currDeviceString + this.selAdapter[i].id)) + ' ' + (await this.getInitValue(currDeviceString + this.selAdapter[i].id2));
					break;

				// Get ID with short currDeviceString from objectjson
				case 'hueExt':
				case 'hmrpc':
				case 'nukiExt':
				case 'wled':
				case 'mqttNuki':
				case 'loqedSmartLock':
				case 'viessmann':
				case 'homekitController':
					if (shortDeviceObject && typeof shortDeviceObject === 'object' && shortDeviceObject.common) {
						deviceName = shortDeviceObject.common.name;
					}
					break;

				// Get ID with short short currDeviceString from objectjson (HMiP Devices)
				case 'hmiP':
					if (shortshortDeviceObject && typeof shortshortDeviceObject === 'object' && shortshortDeviceObject.common) {
						deviceName = shortshortDeviceObject.common.name;
					}
					break;

				// Get ID with short currDeviceString from datapoint
				case 'mihomeVacuum':
				case 'roomba':
					folderName = shortCurrDeviceString.slice(shortCurrDeviceString.lastIndexOf('.') + 1);
					deviceID = await this.getInitValue(shortCurrDeviceString + this.selAdapter[i].id);
					deviceName = `I${folderName} ${deviceID}`;
					break;

				//Get ID of foldername
				case 'tado':
				case 'wifilight':
				case 'fullybrowserV3':
					deviceName = currDeviceString.slice(currDeviceString.lastIndexOf('.') + 1);
					break;

				// Format Device name
				case 'sureflap':
					if (deviceObject && typeof deviceObject === 'object' && deviceObject.common) {
						deviceName = deviceObject.common.name
							// @ts-ignore FIXME: fix syntax error
							.replace(/'/g, '')
							.replace(/\(\d+\)/g, '')
							.trim()
							.replace('Hub', 'Hub -')
							.replace('Device', 'Device -');
					}
					break;

				//Get ID of foldername
				case 'yeelight':
					deviceName = shortCurrDeviceString.slice(shortCurrDeviceString.lastIndexOf('.') + 1);
					break;

				// Get ID with main selektor from objectjson
				default:
					if (this.selAdapter[i].id !== 'none' || this.selAdapter[i].id !== undefined) deviceName = await this.getInitValue(currDeviceString + this.selAdapter[i].id);
					if (deviceName === null || deviceName === undefined) {
						if (deviceObject && typeof deviceObject === 'object' && deviceObject.common) {
							deviceName = deviceObject.common.name;
						}
					}
					break;
			}
			return deviceName;
		} catch (error) {
			this.log.error(`[getDeviceName] - ${error}`);
		}
	}

	/**
	 * calculate Signalstrength
	 * @param {object} deviceQualityState - State value
	 * @param {object} adapterID - adapter name
	 */
	async calculateSignalStrength(deviceQualityState, adapterID) {
		let linkQuality;
		let linkQualityRaw;
		let mqttNukiValue;

		if (deviceQualityState != null) {
			const { val } = deviceQualityState;

			if (typeof val === 'number') {
				if (this.config.trueState) {
					linkQuality = val;
				} else {
					switch (adapterID) {
						case 'roomba':
						case 'sonoff':
						case 'smartgarden':
							linkQuality = `${val}%`;
							linkQualityRaw = val;
							break;
						case 'lupusec':
						case 'fullybrowserV3':
							linkQuality = val;
							break;
						default:
							if (val <= -255) {
								linkQuality = ' - ';
							} else if (val < 0) {
								linkQualityRaw = Math.min(Math.max(2 * (val + 100), 0), 100);
								linkQuality = `${linkQualityRaw}%`;
							} else if (val >= 0) {
								linkQualityRaw = parseFloat(((100 / 255) * val).toFixed(0));
								linkQuality = `${linkQualityRaw}%`;
							}
							break;
					}
				}
			} else if (typeof val === 'string') {
				switch (adapterID) {
					case 'netatmo':
						linkQuality = val;
						break;
					case 'nukiExt':
						linkQuality = ' - ';
						break;
					case 'mqttNuki':
						linkQuality = val;
						mqttNukiValue = parseInt(linkQuality);
						if (this.config.trueState) {
							linkQuality = val;
						} else if (mqttNukiValue < 0) {
							linkQualityRaw = Math.min(Math.max(2 * (mqttNukiValue + 100), 0), 100);
							linkQuality = `${linkQualityRaw}%`;
						}
						break;
				}
			}
		} else {
			linkQuality = ' - ';
		}
		return [linkQuality, linkQualityRaw];
	}

	/**
	 * get battery data
	 * @param {object} deviceBatteryState - State value
	 * @param {object} deviceLowBatState - State value
	 * @param {object} faultReportingState - State value
	 * @param {object} adapterID - adapter name
	 */
	async getBatteryData(deviceBatteryState, deviceLowBatState, faultReportingState, adapterID) {
		let batteryHealth = '-';
		let isBatteryDevice = false;
		let batteryHealthRaw;
		let batteryHealthUnitRaw;

		if (adapterID === 'hmrpc') {
			if (deviceBatteryState === undefined) {
				if (faultReportingState !== undefined && faultReportingState !== 6) {
					batteryHealth = 'ok';
					isBatteryDevice = true;
				} else if (deviceLowBatState !== undefined && deviceLowBatState !== 1) {
					batteryHealth = 'ok';
					isBatteryDevice = true;
				} else if (deviceLowBatState !== undefined) {
					batteryHealth = 'low';
					isBatteryDevice = true;
				}
			} else if (deviceBatteryState !== 0 && deviceBatteryState < 6) {
				batteryHealth = `${deviceBatteryState}V`;
				batteryHealthRaw = deviceBatteryState;
				batteryHealthUnitRaw = 'V';
				isBatteryDevice = true;
			}
		} else {
			if (deviceBatteryState === undefined) {
				if (deviceLowBatState !== undefined) {
					if (deviceLowBatState !== true && deviceLowBatState !== 'NORMAL' && deviceLowBatState !== 1) {
						batteryHealth = 'ok';
						isBatteryDevice = true;
					} else if (deviceLowBatState !== true) {
						batteryHealth = 'low';
						isBatteryDevice = true;
					}
				}
			} else {
				batteryHealth = `${deviceBatteryState}%`;
				batteryHealthRaw = deviceBatteryState;
				batteryHealthUnitRaw = '%';
				isBatteryDevice = true;
			}
		}

		return [batteryHealth, isBatteryDevice, batteryHealthRaw, batteryHealthUnitRaw];
	}

	/**
	 * set low bat indicator
	 * @param {object} deviceBatteryState
	 * @param {object} deviceLowBatState
	 * @param {object} faultReportState
	 * @param {object} adapterID
	 */

	async setLowbatIndicator(deviceBatteryState, deviceLowBatState, faultReportState, adapterID) {
		let lowBatIndicator = false;

		if (deviceLowBatState !== undefined || faultReportState !== undefined) {
			switch (adapterID) {
				case 'hmrpc':
					if (deviceLowBatState === 1 || deviceLowBatState === true || faultReportState === 6) {
						lowBatIndicator = true;
					}
					break;
				default:
					if (typeof deviceLowBatState === 'number' && deviceLowBatState === 0) {
						lowBatIndicator = true;
					} else if (typeof deviceLowBatState === 'string' && deviceLowBatState !== 'NORMAL') {
						lowBatIndicator = true;
					} else if (typeof deviceLowBatState === 'boolean' && deviceLowBatState) {
						lowBatIndicator = true;
					}
			}
		} else if (deviceBatteryState < this.config.minWarnBatterie) {
			lowBatIndicator = true;
		}

		return lowBatIndicator;
	}

	/**
	 * get Last Contact
	 * @param {object} selector - Selector
	 */
	async getLastContact(selector) {
		const lastContact = this.getTimestamp(selector);
		let lastContactString;

		lastContactString = `${this.formatDate(new Date(selector), 'hh:mm')}`;
		if (Math.round(lastContact) > 100) {
			lastContactString = `${Math.round(lastContact / 60)} ${translations.hours[this.userSystemLanguage]}`;
		}
		if (Math.round(lastContact / 60) > 48) {
			lastContactString = `${Math.round(lastContact / 60 / 24)} ${translations.days[this.userSystemLanguage]}`;
		}
		return lastContactString;
	}

	/**
	 * get online state and time
	 * @param {object} timeSelector - device Timeselector
	 * @param {string} adapterID - ID of Adapter
	 * @param {string} unreachDP - Datapoint of Unreach
	 * @param {object} linkQuality - Linkquality Value
	 * @param {object} deviceUnreachState - State of deviceUnreach datapoint
	 * @param {string} deviceStateSelectorDP - Selector of device state (like .state)
	 * @param {string} rssiPeerSelectorDP - HM RSSI Peer Datapoint
	 */
	async getOnlineState(timeSelector, adapterID, unreachDP, linkQuality, deviceUnreachState, deviceStateSelectorDP, rssiPeerSelectorDP) {
		let lastContactString;
		let deviceState = 'Online';

		try {
			const deviceTimeSelector = await this.getForeignStateAsync(timeSelector);
			const deviceUnreachSelector = await this.getForeignStateAsync(unreachDP);
			const deviceStateSelector = await this.getForeignStateAsync(deviceStateSelectorDP); // for hmrpc devices
			const rssiPeerSelector = await this.getForeignStateAsync(rssiPeerSelectorDP);
			const lastDeviceUnreachStateChange = deviceUnreachSelector != undefined ? this.getTimestamp(deviceUnreachSelector.lc) : this.getTimestamp(timeSelector.ts);
			//  If there is no contact since user sets minutes add device in offline list
			// calculate to days after 48 hours
			switch (unreachDP) {
				case 'none':
					if (deviceTimeSelector) lastContactString = await this.getLastContact(deviceTimeSelector.ts);
					break;

				default:
					//State changed
					if (adapterID === 'hmrpc') {
						if (linkQuality !== ' - ' && deviceTimeSelector) {
							if (deviceUnreachState === 1) {
								lastContactString = await this.getLastContact(deviceTimeSelector.lc);
							} else {
								lastContactString = await this.getLastContact(deviceTimeSelector.ts);
							}
						} else {
							if (deviceStateSelector) {
								// because old hm devices don't send rssi states
								lastContactString = await this.getLastContact(deviceStateSelector.ts);
							} else if (rssiPeerSelector) {
								// because old hm sensors don't send rssi/state values
								lastContactString = await this.getLastContact(rssiPeerSelector.ts);
							}
						}
					} else {
						if ((!deviceUnreachState || deviceUnreachState === 0) && deviceTimeSelector) {
							lastContactString = await this.getLastContact(deviceTimeSelector.lc);
						} else {
							if (deviceTimeSelector) lastContactString = await this.getLastContact(deviceTimeSelector.ts);
						}
						break;
					}
			}

			/*=============================================
					=            Set Online Status             =
					=============================================*/
			let lastContact;
			if (deviceTimeSelector) lastContact = this.getTimestamp(deviceTimeSelector.ts);

			if (this.configMaxMinutes !== undefined) {
				switch (adapterID) {
					case 'hmrpc':
						if (this.configMaxMinutes[adapterID] <= 0) {
							if (deviceUnreachState === 1) {
								deviceState = 'Offline'; //set online state to offline
								if (linkQuality !== ' - ') linkQuality = '0%'; // set linkQuality to nothing
							}
						} else if (lastDeviceUnreachStateChange > this.configMaxMinutes[adapterID] && deviceUnreachState === 1) {
							deviceState = 'Offline'; //set online state to offline
							if (linkQuality !== ' - ') linkQuality = '0%'; // set linkQuality to nothing
						}
						break;
					case 'proxmox':
						if (this.configMaxMinutes[adapterID] <= 0) {
							if (deviceUnreachState !== 'running' && deviceUnreachState !== 'online') {
								deviceState = 'Offline'; //set online state to offline
								if (linkQuality !== ' - ') linkQuality = '0%'; // set linkQuality to nothing
							}
						} else if (lastDeviceUnreachStateChange > this.configMaxMinutes[adapterID] && deviceUnreachState !== 'running' && deviceUnreachState !== 'online') {
							deviceState = 'Offline'; //set online state to offline
							if (linkQuality !== ' - ') linkQuality = '0%'; // set linkQuality to nothing
						}
						break;
					case 'hmiP':
					case 'maxcube':
						if (this.configMaxMinutes[adapterID] <= 0) {
							if (deviceUnreachState) {
								deviceState = 'Offline'; //set online state to offline
								if (linkQuality !== ' - ') linkQuality = '0%'; // set linkQuality to nothing
							}
						} else if (lastDeviceUnreachStateChange > this.configMaxMinutes[adapterID] && deviceUnreachState) {
							deviceState = 'Offline'; //set online state to offline
							if (linkQuality !== ' - ') linkQuality = '0%'; // set linkQuality to nothing
						}
						break;
					case 'apcups':
					case 'hue':
					case 'hueExt':
					case 'ping':
					case 'deconz':
					case 'shelly':
					case 'sonoff':
					case 'tradfri':
					case 'unifi':
					case 'zigbee':
					case 'zigbee2MQTT':
						if (this.configMaxMinutes[adapterID] <= 0) {
							if (!deviceUnreachState) {
								deviceState = 'Offline'; //set online state to offline
								if (linkQuality !== ' - ') linkQuality = '0%'; // set linkQuality to nothing
							}
						} else if (!deviceUnreachState && lastDeviceUnreachStateChange > this.configMaxMinutes[adapterID]) {
							deviceState = 'Offline'; //set online state to offline
							if (linkQuality !== ' - ') linkQuality = '0%'; // set linkQuality to nothing
						}
						break;
					case 'mqttClientZigbee2Mqtt':
						if (this.configMaxMinutes[adapterID] <= 0) {
							if (deviceUnreachState !== 'online') {
								deviceState = 'Offline'; //set online state to offline
								if (linkQuality !== ' - ') linkQuality = '0%'; // set linkQuality to nothing
							}
						} else if (deviceUnreachState !== 'online' && lastDeviceUnreachStateChange > this.configMaxMinutes[adapterID]) {
							deviceState = 'Offline'; //set online state to offline
							if (linkQuality !== ' - ') linkQuality = '0%'; // set linkQuality to nothing
						}
						break;
					case 'mihome':
						if (deviceUnreachState !== undefined) {
							if (this.configMaxMinutes[adapterID] <= 0) {
								if (!deviceUnreachState) {
									deviceState = 'Offline'; //set online state to offline
									if (linkQuality !== ' - ') linkQuality = '0%'; // set linkQuality to nothing
								}
							} else if (lastContact && lastContact > this.configMaxMinutes[adapterID]) {
								deviceState = 'Offline'; //set online state to offline
								if (linkQuality !== ' - ') linkQuality = '0%'; // set linkQuality to nothing
							}
						} else {
							if (this.config.mihomeMaxMinutes <= 0) {
								if (this.configMaxMinutes[adapterID] <= 0) {
									deviceState = 'Offline'; //set online state to offline
									if (linkQuality !== ' - ') linkQuality = '0%'; // set linkQuality to nothing
								}
							} else if (lastContact && lastContact > this.configMaxMinutes[adapterID]) {
								deviceState = 'Offline'; //set online state to offline
								if (linkQuality !== ' - ') linkQuality = '0%'; // set linkQuality to nothing
							}
						}
						break;
					case 'smartgarden':
						if (this.configMaxMinutes[adapterID] <= 0) {
							if (deviceUnreachState === 'OFFLINE') {
								deviceState = 'Offline'; //set online state to offline
								if (linkQuality !== ' - ') linkQuality = '0%'; // set linkQuality to nothing
							}
						} else if (deviceUnreachState === 'OFFLINE' && lastDeviceUnreachStateChange > this.configMaxMinutes[adapterID]) {
							deviceState = 'Offline'; //set online state to offline
							if (linkQuality !== ' - ') linkQuality = '0%'; // set linkQuality to nothing
						}
						break;
					default:
						if (this.configMaxMinutes[adapterID] <= 0) {
							if (!deviceUnreachState) {
								deviceState = 'Offline'; //set online state to offline
								if (linkQuality !== ' - ') linkQuality = '0%'; // set linkQuality to nothing
							}
						} else if (lastContact && lastContact > this.configMaxMinutes[adapterID]) {
							deviceState = 'Offline'; //set online state to offline
							if (linkQuality !== ' - ') linkQuality = '0%'; // set linkQuality to nothing
						}
						break;
				}
			}
			return [lastContactString, deviceState, linkQuality];
		} catch (error) {
			this.log.error(`[getLastContact] - ${error}`);
		}
	}

	/**
	 * when was last contact of device
	 */
	async checkLastContact() {
		for (const [device, deviceData] of this.listAllDevicesRaw.entries()) {
			if (deviceData.instancedeviceConnected !== false) {
				const oldContactState = deviceData.Status;
				deviceData.UnreachState = await this.getInitValue(deviceData.UnreachDP);
				const contactData = await this.getOnlineState(
					deviceData.timeSelector,
					deviceData.adapterID,
					deviceData.UnreachDP,
					deviceData.SignalStrength,
					deviceData.UnreachState,
					deviceData.DeviceStateSelectorDP,
					deviceData.rssiPeerSelectorDP,
				);
				if (contactData !== undefined) {
					deviceData.LastContact = contactData[0];
					deviceData.Status = contactData[1];
					deviceData.linkQuality = contactData[2];
				}
				if (this.config.checkSendOfflineMsg && oldContactState !== deviceData.Status && !this.blacklistNotify.includes(deviceData.Path)) {
					await this.sendStateNotifications('Devices', 'onlineStateDevice', device);
				}
			}
		}
	}

	/**
	 * @param {any} adapterID
	 * @param {string | number | boolean | null} deviceUpdateSelector
	 */
	async checkDeviceUpdate(adapterID, deviceUpdateSelector) {
		let isUpgradable;

		switch (adapterID) {
			case 'hmiP':
				if (deviceUpdateSelector === 'UPDATE_AVAILABLE') {
					isUpgradable = true;
				} else {
					isUpgradable = false;
				}
				break;
			default:
				if (deviceUpdateSelector !== null && typeof deviceUpdateSelector === 'boolean') {
					if (deviceUpdateSelector) {
						isUpgradable = true;
					} else if (!deviceUpdateSelector) {
						isUpgradable = false;
					}
				}
		}

		return isUpgradable;
	}

	/**
	 * Create Lists
	 * @param {string | undefined} [adptName]
	 */
	async createLists(adptName) {
		this.linkQualityDevices = [];
		this.batteryPowered = [];
		this.batteryLowPowered = [];
		this.listAllDevicesUserRaw = [];
		this.listAllDevices = [];
		this.offlineDevices = [];
		this.batteryLowPoweredRaw = [];
		this.offlineDevicesRaw = [];
		this.upgradableDevicesRaw = [];
		this.upgradableList = [];

		if (adptName === undefined) {
			adptName = '';
		}

		for (const deviceData of this.listAllDevicesRaw.values()) {
			/*----------  fill raw lists  ----------*/
			// low bat list
			if (deviceData.LowBat && deviceData.Status !== 'Offline') {
				this.batteryLowPoweredRaw.push({
					Path: deviceData.Path,
					Device: deviceData.Device,
					Adapter: deviceData.Adapter,
					Battery: deviceData.Battery,
				});
			}
			// offline raw list
			if (deviceData.Status === 'Offline') {
				this.offlineDevicesRaw.push({
					Path: deviceData.Path,
					Device: deviceData.Device,
					Adapter: deviceData.Adapter,
					LastContact: deviceData.LastContact,
				});
			}

			// upgradable raw list
			if (deviceData.Upgradable === true) {
				this.upgradableDevicesRaw.push({
					Path: deviceData.Path,
					Device: deviceData.Device,
					Adapter: deviceData.Adapter,
				});
			}

			if (adptName === '' && !this.blacklistLists.includes(deviceData.Path)) {
				await this.theLists(deviceData);
			}

			if (this.config.createOwnFolder && adptName !== '') {
				if (!deviceData.adapterID.includes(adptName)) continue;
				/*----------  fill user lists for each adapter  ----------*/
				if (this.blacklistAdapterLists.includes(deviceData.Path)) continue;
				await this.theLists(deviceData);
			}
		}
		await this.countDevices();
	}

	/**
	 * fill the lists for user
	 * @param {object} device
	 */
	async theLists(device) {
		// Raw List with all devices for user
		this.listAllDevicesUserRaw.push({
			Device: device.Device,
			Adapter: device.Adapter,
			Instance: device.instance,
			'Instance connected': device.instancedeviceConnected,
			isBatteryDevice: device.isBatteryDevice,
			Battery: device.Battery,
			BatteryRaw: device.BatteryRaw,
			BatteryUnitRaw: device.BatteryUnitRaw,
			isLowBat: device.LowBat,
			'Signal strength': device.SignalStrength,
			'Signal strength Raw': device.SignalStrengthRaw,
			'Last contact': device.LastContact,
			'Update Available': device.Upgradable,
			Status: device.Status,
		});

		// List with all devices
		this.listAllDevices.push({
			[translations.Device[this.userSystemLanguage]]: device.Device,
			[translations.Adapter[this.userSystemLanguage]]: device.Adapter,
			[translations.Battery[this.userSystemLanguage]]: device.Battery,
			[translations.Signal_strength[this.userSystemLanguage]]: device.SignalStrength,
			[translations.Last_Contact[this.userSystemLanguage]]: device.LastContact,
			[translations.Status[this.userSystemLanguage]]: device.Status,
		});

		// LinkQuality lists
		if (device.SignalStrength != ' - ') {
			this.linkQualityDevices.push({
				[translations.Device[this.userSystemLanguage]]: device.Device,
				[translations.Adapter[this.userSystemLanguage]]: device.Adapter,
				[translations.Signal_strength[this.userSystemLanguage]]: device.SignalStrength,
			});
		}

		// Battery lists
		if (device.isBatteryDevice) {
			this.batteryPowered.push({
				[translations.Device[this.userSystemLanguage]]: device.Device,
				[translations.Adapter[this.userSystemLanguage]]: device.Adapter,
				[translations.Battery[this.userSystemLanguage]]: device.Battery,
				[translations.Status[this.userSystemLanguage]]: device.Status,
			});
		}

		// Low Bat lists
		if (device.LowBat && device.Status !== 'Offline') {
			this.batteryLowPowered.push({
				[translations.Device[this.userSystemLanguage]]: device.Device,
				[translations.Adapter[this.userSystemLanguage]]: device.Adapter,
				[translations.Battery[this.userSystemLanguage]]: device.Battery,
			});
		}

		// Offline List
		if (device.Status === 'Offline') {
			this.offlineDevices.push({
				[translations.Device[this.userSystemLanguage]]: device.Device,
				[translations.Adapter[this.userSystemLanguage]]: device.Adapter,
				[translations.Last_Contact[this.userSystemLanguage]]: device.LastContact,
			});
		}

		// Device update List
		if (device.Upgradable === true || device.Upgradable === 1) {
			this.upgradableList.push({
				[translations.Device[this.userSystemLanguage]]: device.Device,
				[translations.Adapter[this.userSystemLanguage]]: device.Adapter,
			});
		}
	}

	/**
	 * Count devices for each type
	 */
	async countDevices() {
		// Count how many devices with link Quality
		this.linkQualityCount = this.linkQualityDevices.length;

		// Count how many devcies are offline
		this.offlineDevicesCount = this.offlineDevices.length;

		// Count how many devices are with battery
		this.batteryPoweredCount = this.batteryPowered.length;

		// 3d. Count how many devices are with low battery
		this.lowBatteryPoweredCount = this.batteryLowPowered.length;

		// Count how many devices are exists
		this.deviceCounter = this.listAllDevices.length;

		// Count how many devices has update available
		this.upgradableDevicesCount = this.upgradableList.length;
	}

	/**
	 * @param {string} [adptName] - Adaptername
	 */
	async writeDatapoints(adptName) {
		// fill the datapoints
		this.log.debug(`Start the function: ${this.writeDatapoints.name}`);

		try {
			let dpSubFolder;
			//write the datapoints in subfolders with the adaptername otherwise write the dP's in the root folder
			if (adptName) {
				dpSubFolder = adptName + '.';
			} else {
				dpSubFolder = '';
			}

			// Write Datapoints for counts
			await this.setStateChangedAsync(`devices.${dpSubFolder}offlineCount`, { val: this.offlineDevicesCount, ack: true });
			await this.setStateChangedAsync(`devices.${dpSubFolder}countAll`, { val: this.deviceCounter, ack: true });
			await this.setStateChangedAsync(`devices.${dpSubFolder}batteryCount`, { val: this.batteryPoweredCount, ack: true });
			await this.setStateChangedAsync(`devices.${dpSubFolder}lowBatteryCount`, { val: this.lowBatteryPoweredCount, ack: true });
			await this.setStateChangedAsync(`devices.${dpSubFolder}upgradableCount`, { val: this.upgradableDevicesCount, ack: true });
			// List all devices
			if (this.deviceCounter === 0) {
				// if no device is count, write the JSON List with default value
				this.listAllDevices = [
					{
						[translations.Device[this.userSystemLanguage]]: '--none--',
						[translations.Adapter[this.userSystemLanguage]]: '',
						[translations.Battery[this.userSystemLanguage]]: '',
						[translations.Signal_strength[this.userSystemLanguage]]: '',
						[translations.Last_Contact[this.userSystemLanguage]]: '',
						[translations.Status[this.userSystemLanguage]]: '',
					},
				];
				this.listAllDevicesUserRaw = [
					{
						Device: '--none--',
						Adapter: '',
						Instance: '',
						'Instance connected': '',
						isBatteryDevice: '',
						Battery: '',
						BatteryRaw: '',
						isLowBat: '',
						'Signal strength': '',
						'Last contact': '',
						UpdateAvailable: '',
						Status: '',
					},
				];
			}
			await this.setStateChangedAsync(`devices.${dpSubFolder}listAll`, { val: JSON.stringify(this.listAllDevices), ack: true });
			await this.setStateChangedAsync(`devices.${dpSubFolder}listAllRawJSON`, { val: JSON.stringify(this.listAllDevicesUserRaw), ack: true });

			// List link quality
			if (this.linkQualityCount === 0) {
				// if no device is count, write the JSON List with default value
				this.linkQualityDevices = [
					{
						[translations.Device[this.userSystemLanguage]]: '--none--',
						[translations.Adapter[this.userSystemLanguage]]: '',
						[translations.Signal_strength[this.userSystemLanguage]]: '',
					},
				];
			}
			//write JSON list
			await this.setStateChangedAsync(`devices.${dpSubFolder}linkQualityList`, {
				val: JSON.stringify(this.linkQualityDevices),
				ack: true,
			});

			// List offline devices
			if (this.offlineDevicesCount === 0) {
				// if no device is count, write the JSON List with default value
				this.offlineDevices = [
					{
						[translations.Device[this.userSystemLanguage]]: '--none--',
						[translations.Adapter[this.userSystemLanguage]]: '',
						[translations.Last_Contact[this.userSystemLanguage]]: '',
					},
				];
			}
			//write JSON list
			await this.setStateChangedAsync(`devices.${dpSubFolder}offlineList`, {
				val: JSON.stringify(this.offlineDevices),
				ack: true,
			});

			// List updatable
			if (this.upgradableDevicesCount === 0) {
				// if no device is count, write the JSON List with default value
				this.upgradableList = [
					{
						[translations.Device[this.userSystemLanguage]]: '--none--',
						[translations.Adapter[this.userSystemLanguage]]: '',
						[translations.Last_Contact[this.userSystemLanguage]]: '',
					},
				];
			}
			//write JSON list
			await this.setStateChangedAsync(`devices.${dpSubFolder}upgradableList`, {
				val: JSON.stringify(this.upgradableList),
				ack: true,
			});

			// List battery powered
			if (this.batteryPoweredCount === 0) {
				// if no device is count, write the JSON List with default value
				this.batteryPowered = [
					{ [translations.Device[this.userSystemLanguage]]: '--none--', [translations.Adapter[this.userSystemLanguage]]: '', [translations.Battery[this.userSystemLanguage]]: '' },
				];
			}
			//write JSON list
			await this.setStateChangedAsync(`devices.${dpSubFolder}batteryList`, {
				val: JSON.stringify(this.batteryPowered),
				ack: true,
			});

			// list battery low powered
			if (this.lowBatteryPoweredCount === 0) {
				// if no device is count, write the JSON List with default value
				this.batteryLowPowered = [
					{ [translations.Device[this.userSystemLanguage]]: '--none--', [translations.Adapter[this.userSystemLanguage]]: '', [translations.Battery[this.userSystemLanguage]]: '' },
				];
			}
			//write JSON list
			await this.setStateChangedAsync(`devices.${dpSubFolder}lowBatteryList`, {
				val: JSON.stringify(this.batteryLowPowered),
				ack: true,
			});

			// set booleans datapoints
			if (this.offlineDevicesCount === 0) {
				await this.setStateChangedAsync(`devices.${dpSubFolder}oneDeviceOffline`, {
					val: false,
					ack: true,
				});
			} else {
				await this.setStateChangedAsync(`devices.${dpSubFolder}oneDeviceOffline`, {
					val: true,
					ack: true,
				});
			}

			if (this.lowBatteryPoweredCount === 0) {
				await this.setStateChangedAsync(`devices.${dpSubFolder}oneDeviceLowBat`, {
					val: false,
					ack: true,
				});
			} else {
				await this.setStateChangedAsync(`devices.${dpSubFolder}oneDeviceLowBat`, {
					val: true,
					ack: true,
				});
			}

			if (this.upgradableDevicesCount === 0) {
				await this.setStateChangedAsync(`devices.${dpSubFolder}oneDeviceUpdatable`, {
					val: false,
					ack: true,
				});
			} else {
				await this.setStateChangedAsync(`devices.${dpSubFolder}oneDeviceUpdatable`, {
					val: true,
					ack: true,
				});
			}

			//write HTML list
			if (this.configCreateHtmlList) {
				await this.setStateChangedAsync(`devices.${dpSubFolder}linkQualityListHTML`, {
					val: await this.createListHTML('linkQualityList', this.linkQualityDevices, this.linkQualityCount, null),
					ack: true,
				});
				await this.setStateChangedAsync(`devices.${dpSubFolder}offlineListHTML`, {
					val: await this.createListHTML('offlineList', this.offlineDevices, this.offlineDevicesCount, null),
					ack: true,
				});
				await this.setStateChangedAsync(`devices.${dpSubFolder}batteryListHTML`, {
					val: await this.createListHTML('batteryList', this.batteryPowered, this.batteryPoweredCount, false),
					ack: true,
				});
				await this.setStateChangedAsync(`devices.${dpSubFolder}lowBatteryListHTML`, {
					val: await this.createListHTML('batteryList', this.batteryLowPowered, this.lowBatteryPoweredCount, true),
					ack: true,
				});
				if (this.config.checkAdapterInstances) {
					await this.setStateChangedAsync(`adapterAndInstances.HTML_Lists.listAllInstancesHTML`, {
						val: await this.createListHTMLInstances('allInstancesList', this.listAllInstances, this.countAllInstances),
						ack: true,
					});
					await this.setStateChangedAsync(`adapterAndInstances.HTML_Lists.listAllActiveInstancesHTML`, {
						val: await this.createListHTMLInstances('allActiveInstancesList', this.listAllActiveInstances, this.countAllActiveInstances),
						ack: true,
					});
					await this.setStateChangedAsync(`adapterAndInstances.HTML_Lists.listInstancesErrorHTML`, {
						val: await this.createListHTMLInstances('errorInstanceList', this.listErrorInstance, this.countErrorInstance),
						ack: true,
					});
					await this.setStateChangedAsync(`adapterAndInstances.HTML_Lists.listDeactivatedInstancesHTML`, {
						val: await this.createListHTMLInstances('deactivatedInstanceList', this.listDeactivatedInstances, this.countDeactivatedInstances),
						ack: true,
					});
					await this.setStateChangedAsync(`adapterAndInstances.HTML_Lists.listAdapterUpdatesHTML`, {
						val: await this.createListHTMLInstances('updateAdapterList', this.listAdapterUpdates, this.countAdapterUpdates),
						ack: true,
					});
				}
			}

			// create timestamp of last run
			const lastCheck = this.formatDate(new Date(), 'DD.MM.YYYY') + ' - ' + this.formatDate(new Date(), 'hh:mm:ss');
			await this.setStateChangedAsync('lastCheck', lastCheck, true);
		} catch (error) {
			this.log.error(`[writeDatapoints] - ${error}`);
		}
		this.log.debug(`Function finished: ${this.writeDatapoints.name}`);
	} //<--End  of writing Datapoints

	/**
	 * @param {string | string[]} id
	 * @param {ioBroker.State} state
	 */
	async renewDeviceData(id, state) {
		let batteryData;
		let signalData;
		let oldLowBatState;
		let contactData;
		let oldStatus;
		let isLowBatValue;

		const deviceID = id.slice(0, id.lastIndexOf('.') + 1 - 1);
		const deviceData = this.listAllDevicesRaw.get(deviceID);

		if (deviceData) {
			// On statechange update available datapoint
			switch (id) {
				// device connection
				case deviceData.instanceDeviceConnectionDP:
					if (state.val !== deviceData.instancedeviceConnected) {
						deviceData.instancedeviceConnected = state.val;
					}
					break;

				// device updates
				case deviceData.UpdateDP:
					if (state.val !== deviceData.Upgradable) {
						deviceData.Upgradable = await this.checkDeviceUpdate(deviceData.adapterID, state.val);
						if (deviceData.Upgradable === true) {
							if (this.config.checkSendDeviceUpgrade && !this.blacklistNotify.includes(deviceData.Path)) {
								await this.sendStateNotifications('Devices', 'updateDevice', deviceID);
							}
						}
					}
					break;

				// device signal
				case deviceData.SignalStrengthDP:
					signalData = await this.calculateSignalStrength(state, deviceData.adapterID);
					deviceData.SignalStrength = signalData[0];

					break;

				// device battery
				case deviceData.batteryDP:
					if (deviceData.isBatteryDevice) {
						oldLowBatState = deviceData.LowBat;
						if (state.val === 0 && deviceData.BatteryRaw >= 5) return;
						batteryData = await this.getBatteryData(state.val, oldLowBatState, deviceData.faultReport, deviceData.adapterID);

						deviceData.Battery = batteryData[0];
						deviceData.BatteryRaw = batteryData[2];
						deviceData.BatteryUnitRaw = batteryData[3];
						if (deviceData.LowBatDP !== 'none') {
							isLowBatValue = await this.getInitValue(deviceData.LowBatDP);
						} else {
							isLowBatValue = undefined;
						}
						deviceData.LowBat = await this.setLowbatIndicator(state.val, isLowBatValue, deviceData.faultReport, deviceData.adapterID);

						if (deviceData.LowBat && oldLowBatState !== deviceData.LowBat) {
							if (this.config.checkSendBatteryMsg && !this.blacklistNotify.includes(deviceData.Path)) {
								await this.sendStateNotifications('Devices', 'lowBatDevice', deviceID);
							}
						}
					}
					break;

				// device low bat
				case deviceData.LowBatDP:
					if (deviceData.isBatteryDevice) {
						oldLowBatState = deviceData.LowBat;
						batteryData = await this.getBatteryData(deviceData.BatteryRaw, state.val, deviceData.faultReport, deviceData.adapterID);
						deviceData.Battery = batteryData[0];
						deviceData.BatteryRaw = batteryData[2];
						deviceData.BatteryUnitRaw = batteryData[3];
						deviceData.LowBat = await this.setLowbatIndicator(deviceData.BatteryRaw, state.val, deviceData.faultReport, deviceData.adapterID);

						if (deviceData.LowBat && oldLowBatState !== deviceData.LowBat) {
							if (this.config.checkSendBatteryMsg && !this.blacklistNotify.includes(deviceData.Path)) {
								await this.sendStateNotifications('Devices', 'lowBatDevice', deviceID);
							}
						}
					}
					break;

				//device error / fault reports
				case deviceData.faultReportDP:
					if (deviceData.isBatteryDevice) {
						oldLowBatState = deviceData.LowBat;
						batteryData = await this.getBatteryData(deviceData.BatteryRaw, oldLowBatState, state.val, deviceData.adapterID);

						deviceData.Battery = batteryData[0];
						deviceData.BatteryRaw = batteryData[2];
						deviceData.BatteryUnitRaw = batteryData[3];
						deviceData.LowBat = await this.setLowbatIndicator(deviceData.BatteryRaw, undefined, state.val, deviceData.adapterID);

						if (deviceData.LowBat && oldLowBatState !== deviceData.LowBat) {
							if (this.config.checkSendBatteryMsg && !this.blacklistNotify.includes(deviceData.Path)) {
								await this.sendStateNotifications('Devices', 'lowBatDevice', deviceID);
							}
						}
					}
					break;

				// device unreach
				case deviceData.UnreachDP:
					if (deviceData.instanceDeviceConnectionDP.val !== undefined) {
						// check if the generally deviceData connected state is for a while true
						if (deviceData.UnreachState !== state.val) {
							oldStatus = deviceData.Status;
							deviceData.UnreachState = state.val;
							contactData = await this.getOnlineState(
								deviceData.timeSelector,
								deviceData.adapterID,
								deviceData.UnreachDP,
								deviceData.SignalStrength,
								deviceData.UnreachState,
								deviceData.DeviceStateSelectorDP,
								deviceData.rssiPeerSelectorDP,
							);
							if (contactData !== undefined) {
								deviceData.LastContact = contactData[0];
								deviceData.Status = contactData[1];
								deviceData.SignalStrength = contactData[2];
							}
							if (this.config.checkSendOfflineMsg && oldStatus !== deviceData.Status && !this.blacklistNotify.includes(deviceData.Path)) {
								if (deviceData.instanceDeviceConnectionDP.val !== undefined) {
									// check if the generally deviceData connected state is for a while true
									if (await this.getTimestampConnectionDP(deviceData.instanceDeviceConnectionDP, 20000)) {
										await this.sendStateNotifications('Devices', 'onlineStateDevice', deviceID);
									}
								} else {
									await this.sendStateNotifications('Devices', 'onlineStateDevice', deviceID);
								}
							}
						}
					}
					break;
			}
		}
	}

	/**
	 * get instance data
	 *@param {string} instanceObject
	 */
	async getInstanceData(instanceObject) {
		try {
			const instanceAliveDP = await this.getForeignStatesAsync(`${instanceObject}.alive`);

			for (const [id] of Object.entries(instanceAliveDP)) {
				if (!(typeof id === 'string' && id.startsWith(`system.adapter.`))) continue;

				// get instance name
				const instanceID = await this.getInstanceName(id);

				// get instance connected to host data
				const instanceConnectedHostDP = `system.adapter.${instanceID}.connected`;
				const instanceConnectedHostVal = await this.getInitValue(instanceConnectedHostDP);

				// get instance connected to device data
				const instanceConnectedDeviceDP = `${instanceID}.info.connection`;
				let instanceConnectedDeviceVal;
				if (instanceConnectedDeviceDP !== undefined && typeof instanceConnectedDeviceDP === 'boolean') {
					instanceConnectedDeviceVal = await this.getInitValue(instanceConnectedDeviceDP);
				} else {
					instanceConnectedDeviceVal = 'N/A';
				}

				// get adapter version
				const instanceObjectPath = `system.adapter.${instanceID}`;
				let adapterName;
				let adapterVersion;
				let adapterAvailableUpdate = '';
				let instanceMode;
				let scheduleTime = 'N/A';
				const instanceObjectData = await this.getForeignObjectAsync(instanceObjectPath);
				if (instanceObjectData) {
					// @ts-ignore
					adapterName = this.capitalize(instanceObjectData.common.name);
					adapterVersion = instanceObjectData.common.version;
					instanceMode = instanceObjectData.common.mode;

					if (instanceMode === 'schedule') {
						scheduleTime = instanceObjectData.common.schedule;
					}
				}

				await this.getAdapterUpdateData(`admin.*.info.updatesJson`);

				if (this.adapterUpdatesJsonRaw.has(adapterName)) {
					for (const adapter of this.adapterUpdatesJsonRaw.values()) {
						adapterAvailableUpdate = adapter.newVersion;
					}
				} else {
					adapterAvailableUpdate = ' - ';
				}

				let isAlive;
				let isHealthy;
				let instanceStatus;
				if (instanceMode === 'schedule') {
					const instanceStatusRaw = await this.checkScheduleisHealty(instanceID, scheduleTime);
					isAlive = instanceStatusRaw[0];
					isHealthy = instanceStatusRaw[1];
					instanceStatus = instanceStatusRaw[2];
				} else if (instanceMode === 'daemon') {
					const instanceStatusRaw = await this.checkDaemonIsHealthy(instanceID);
					isAlive = instanceStatusRaw[0];
					isHealthy = instanceStatusRaw[1];
					instanceStatus = instanceStatusRaw[2];
				}

				//subscribe to statechanges
				this.subscribeForeignStates(id);
				this.subscribeForeignStates(instanceConnectedHostDP);
				this.subscribeForeignStates(instanceConnectedDeviceDP);
				this.subscribeForeignObjects(`system.adapter.*`);
				// this.subscribeForeignStates('*');
				// this.subscribeForeignObjects('*');

				// create raw list
				this.listInstanceRaw.set(instanceID, {
					Adapter: adapterName,
					instanceObjectPath: instanceObjectPath,
					instanceMode: instanceMode,
					schedule: scheduleTime,
					adapterVersion: adapterVersion,
					updateAvailable: adapterAvailableUpdate,
					isAlive: isAlive,
					isHealthy: isHealthy,
					isConnectedHost: instanceConnectedHostVal,
					isConnectedDevice: instanceConnectedDeviceVal,
					status: instanceStatus,
					checkIsRunning: false,
					aliveDP: `system.adapter.${instanceID}.alive`,
					hostConnectionDP: instanceConnectedHostDP,
					deviceConnectionDP: instanceConnectedDeviceDP,
				});
			}
			await this.createInstanceList();
			await this.writeInstanceDPs();
		} catch (error) {
			this.log.error(`[getInstanceData] - ${error}`);
		}
	}

	/**
	 * get Instances
	 * @param {string} id - Path of alive datapoint
	 */
	async getInstanceName(id) {
		let instance = id;
		instance = instance.slice(15); // remove "system.adapter."
		instance = instance.slice(0, instance.lastIndexOf('.') + 1 - 1); // remove ".alive"
		return instance;
	}

	/**
	 * Check if instance is alive and ok
	 * @param {string} instanceID
	 */
	async checkDaemonIsHealthy(instanceID) {
		const connectedHostState = await this.getInitValue(`system.adapter.${instanceID}.connected`);
		const isAlive = await this.getInitValue(`system.adapter.${instanceID}.alive`);
		let connectedDeviceState = await this.getInitValue(`${instanceID}.info.connection`);

		if (connectedDeviceState === undefined) {
			connectedDeviceState = true;
		}

		let isHealthy = false;
		let instanceStatusString = translations.instance_deactivated[this.userSystemLanguage];

		if (isAlive) {
			if (connectedHostState && connectedDeviceState) {
				isHealthy = true;
				instanceStatusString = translations.instance_okay[this.userSystemLanguage];
			} else if (!connectedHostState) {
				instanceStatusString = translations.not_connected_host[this.userSystemLanguage];
			} else if (!connectedDeviceState) {
				instanceStatusString = translations.not_connected_device[this.userSystemLanguage];
			}
		}

		return [Boolean(isAlive), Boolean(isHealthy), String(instanceStatusString)];
	}

	/**
	 * Check if instance is alive and ok
	 * @param {string} instanceID
	 * @param {number} instanceDeactivationTime
	 */
	async checkDaemonIsAlive(instanceID, instanceDeactivationTime) {
		let isAlive = await this.getInitValue(`system.adapter.${instanceID}.alive`);
		let daemonIsAlive;
		let isHealthy = false;
		let instanceStatusString = isAlive ? translations.instance_activated[this.userSystemLanguage] : translations.instance_deactivated[this.userSystemLanguage];

		if (isAlive) {
			daemonIsAlive = await this.checkDaemonIsHealthy(instanceID);
		} else {
			await this.delay(instanceDeactivationTime);
			daemonIsAlive = await this.checkDaemonIsHealthy(instanceID);
			if (!daemonIsAlive[0]) {
				await this.delay(instanceDeactivationTime);
				daemonIsAlive = await this.checkDaemonIsHealthy(instanceID);
			}
		}

		isAlive = Boolean(daemonIsAlive[0]);
		isHealthy = Boolean(daemonIsAlive[1]);
		instanceStatusString = String(daemonIsAlive[2]);

		return [isAlive, isHealthy, instanceStatusString];
	}

	async checkScheduleisHealty(instanceID, scheduleTime) {
		let lastUpdate;
		let previousCronRun = null;
		let lastCronRun;
		let diff;
		let isAlive = false;
		let isHealthy = false;
		let instanceStatusString = translations.instance_deactivated[this.userSystemLanguage];
		const isAliveSchedule = await this.getForeignStateAsync(`system.adapter.${instanceID}.alive`);

		if (isAliveSchedule) {
			lastUpdate = Math.round((Date.now() - isAliveSchedule.lc) / 1000); // Last state change in seconds
			previousCronRun = this.getPreviousCronRun(scheduleTime); // When was the last cron run
			if (previousCronRun) {
				lastCronRun = Math.round(previousCronRun / 1000); // change distance to last run in seconds
				diff = lastCronRun - lastUpdate;
				if (diff > -300) {
					// if 5 minutes difference exceeded, instance is not alive
					isAlive = true;
					isHealthy = true;
					instanceStatusString = translations.instance_okay[this.userSystemLanguage];
				}
			}
		}

		return [isAlive, isHealthy, instanceStatusString];
	}

	/**
	 * set status for instance
	 * @param {string} instanceMode
	 * @param {string} scheduleTime
	 * @param {any} instanceID
	 */
	async setInstanceStatus(instanceMode, scheduleTime, instanceID) {
		let instanceDeactivationTime = (this.config.offlineTimeInstances * 1000) / 2;
		let instanceErrorTime = (this.config.errorTimeInstances * 1000) / 2;

		const checkScheduleIsHealthy = async () => {
			const scheduleIsAlive = await this.checkScheduleisHealty(instanceID, scheduleTime);
			return {
				isAlive: Boolean(scheduleIsAlive[0]),
				isHealthy: Boolean(scheduleIsAlive[1]),
				instanceStatusString: String(scheduleIsAlive[2]),
			};
		};

		const checkDaemonIsHealthy = async () => {
			const daemonIsAlive = await this.checkDaemonIsHealthy(instanceID);

			if (daemonIsAlive[0] && !daemonIsAlive[1]) {
				await this.delay(instanceErrorTime);
				const daemonIsAliveAfterDelay = await this.checkDaemonIsHealthy(instanceID);

				if (daemonIsAliveAfterDelay[0] && !daemonIsAliveAfterDelay[1]) {
					await this.delay(instanceErrorTime);
					const daemonIsAliveAfterSecondDelay = await this.checkDaemonIsHealthy(instanceID);

					if (daemonIsAliveAfterSecondDelay[0] && !daemonIsAliveAfterSecondDelay[1]) {
						return {
							isAlive: Boolean(daemonIsAliveAfterSecondDelay[0]),
							isHealthy: Boolean(daemonIsAliveAfterSecondDelay[1]),
							instanceStatusString: String(daemonIsAliveAfterSecondDelay[2]),
						};
					}
				}
			}

			const daemonIsNotAlive = await this.checkDaemonIsAlive(instanceID, instanceDeactivationTime);
			return {
				isAlive: Boolean(daemonIsNotAlive[0]),
				isHealthy: Boolean(daemonIsNotAlive[1]),
				instanceStatusString: String(daemonIsNotAlive[2]),
			};
		};

		let isAlive = false;
		let isHealthy = false;
		let instanceStatusString = translations.instance_deactivated[this.userSystemLanguage];

		switch (instanceMode) {
			case 'schedule':
				({ isAlive, isHealthy, instanceStatusString } = await checkScheduleIsHealthy());
				break;
			case 'daemon':
				// check with time the user did define for error and deactivation
				if (this.userTimeInstancesList.has(instanceID)) {
					const userTimeInstances = this.userTimeInstancesList.get(instanceID);
					instanceDeactivationTime = (userTimeInstances.deactivationTime * 1000) / 2;
					instanceErrorTime = (userTimeInstances.errorTime * 1000) / 2;
				}

				({ isAlive, isHealthy, instanceStatusString } = await checkDaemonIsHealthy());
				break;
		}

		return [isAlive, isHealthy, instanceStatusString];
	}

	/**
	 * create adapter update data
	 */
	async createAdapterUpdateData() {
		const adapterUpdateListDP = 'admin.*.info.updatesJson';
		// subscribe to datapoint
		this.subscribeForeignStates(adapterUpdateListDP);

		await this.getAdapterUpdateData(adapterUpdateListDP);

		await this.createAdapterUpdateList();
	}

	/**
	 * create adapter update raw lists
	 * @param {string} adapterUpdateListDP
	 */
	async getAdapterUpdateData(adapterUpdateListDP) {
		// Clear the existing adapter updates data
		this.adapterUpdatesJsonRaw.clear();

		// Fetch the adapter updates list
		const adapterUpdatesListVal = await this.getForeignStatesAsync(adapterUpdateListDP);

		let adapterJsonList;
		let adapterUpdatesJsonPath;

		// Extract adapter data from the list
		for (const [id, value] of Object.entries(adapterUpdatesListVal)) {
			adapterJsonList = this.parseData(value.val);
			adapterUpdatesJsonPath = id;
		}

		// Populate the adapter updates data
		for (const [id, adapterData] of Object.entries(adapterJsonList)) {
			this.adapterUpdatesJsonRaw.set(this.capitalize(id), {
				Path: adapterUpdatesJsonPath,
				newVersion: adapterData.availableVersion,
				oldVersion: adapterData.installedVersion,
			});
		}

		return this.adapterUpdatesJsonRaw;
	}

	/**
	 * create instanceList
	 */
	async createAdapterUpdateList() {
		this.listAdapterUpdates = [];
		this.countAdapterUpdates = 0;

		for (const [adapter, updateData] of this.adapterUpdatesJsonRaw) {
			this.listAdapterUpdates.push({
				[translations.Adapter[this.userSystemLanguage]]: adapter,
				[translations.Available_Version[this.userSystemLanguage]]: updateData.newVersion,
				[translations.Installed_Version[this.userSystemLanguage]]: updateData.oldVersion,
			});
		}
		this.countAdapterUpdates = this.listAdapterUpdates.length;
		await this.writeAdapterUpdatesDPs();
	}

	/**
	 * write datapoints for adapter with updates
	 */
	async writeAdapterUpdatesDPs() {
		// Write Datapoints for counts
		await this.setStateChangedAsync(`adapterAndInstances.countAdapterUpdates`, { val: this.countAdapterUpdates, ack: true });

		if (this.countAdapterUpdates === 0) {
			this.listAdapterUpdates = [
				{
					[translations.Adapter[this.userSystemLanguage]]: '--none--',
					[translations.Available_Version[this.userSystemLanguage]]: '',
					[translations.Installed_Version[this.userSystemLanguage]]: '',
				},
			];
		}
		await this.setStateChangedAsync(`adapterAndInstances.listAdapterUpdates`, { val: JSON.stringify(this.listAdapterUpdates), ack: true });
	}

	/**
	 * create instanceList
	 */
	async createInstanceList() {
		this.listAllInstances = [];
		this.listAllActiveInstances = [];
		this.listDeactivatedInstances = [];
		this.listErrorInstanceRaw = [];
		this.listErrorInstance = [];

		for (const [instance, instanceData] of this.listInstanceRaw) {
			// fill raw list
			if (instanceData.isAlive && !instanceData.isHealthy) {
				this.listErrorInstanceRaw.push({
					Adapter: instanceData.Adapter,
					Instance: instance,
					Mode: instanceData.instanceMode,
					Status: instanceData.status,
				});
			}

			if (this.blacklistInstancesLists.includes(instance)) continue;
			// all instances
			this.listAllInstances.push({
				[translations.Adapter[this.userSystemLanguage]]: instanceData.Adapter,
				[translations.Instance[this.userSystemLanguage]]: instance,
				[translations.Mode[this.userSystemLanguage]]: instanceData.instanceMode,
				[translations.Schedule[this.userSystemLanguage]]: instanceData.schedule,
				[translations.Version[this.userSystemLanguage]]: instanceData.adapterVersion,
				[translations.Updateable[this.userSystemLanguage]]: instanceData.updateAvailable,
				[translations.Status[this.userSystemLanguage]]: instanceData.status,
			});

			if (!instanceData.isAlive) {
				// list with deactivated instances
				this.listDeactivatedInstances.push({
					[translations.Adapter[this.userSystemLanguage]]: instanceData.Adapter,
					[translations.Instance[this.userSystemLanguage]]: instance,
					[translations.Status[this.userSystemLanguage]]: instanceData.status,
				});
			} else {
				// list with active instances
				this.listAllActiveInstances.push({
					[translations.Adapter[this.userSystemLanguage]]: instanceData.Adapter,
					[translations.Instance[this.userSystemLanguage]]: instance,
					[translations.Mode[this.userSystemLanguage]]: instanceData.instanceMode,
					[translations.Schedule[this.userSystemLanguage]]: instanceData.schedule,
					[translations.Status[this.userSystemLanguage]]: instanceData.status,
				});
			}

			// list with error instances
			if (instanceData.isAlive && !instanceData.isHealthy) {
				this.listErrorInstance.push({
					[translations.Adapter[this.userSystemLanguage]]: instanceData.Adapter,
					[translations.Instance[this.userSystemLanguage]]: instance,
					[translations.Mode[this.userSystemLanguage]]: instanceData.instanceMode,
					[translations.Status[this.userSystemLanguage]]: instanceData.status,
				});
			}
		}
		await this.countInstances();
	}

	/**
	 * count instanceList
	 */
	async countInstances() {
		this.countAllInstances = 0;
		this.countAllActiveInstances = 0;
		this.countDeactivatedInstances = 0;
		this.countErrorInstance = 0;

		this.countAllInstances = this.listAllInstances.length;
		this.countAllActiveInstances = this.listAllActiveInstances.length;
		this.countDeactivatedInstances = this.listDeactivatedInstances.length;
		this.countErrorInstance = this.listErrorInstance.length;
	}

	/**
	 * write datapoints for instances list and counts
	 */
	async writeInstanceDPs() {
		// List all instances
		await this.setStateChangedAsync(`adapterAndInstances.listAllInstances`, { val: JSON.stringify(this.listAllInstances), ack: true });
		await this.setStateChangedAsync(`adapterAndInstances.countAllInstances`, { val: this.countAllInstances, ack: true });

		// List all active instances
		await this.setStateChangedAsync(`adapterAndInstances.listAllActiveInstances`, { val: JSON.stringify(this.listAllActiveInstances), ack: true });
		await this.setStateChangedAsync(`adapterAndInstances.countAllActiveInstances`, { val: this.countAllActiveInstances, ack: true });

		// list deactivated instances
		if (this.countDeactivatedInstances === 0) {
			this.listDeactivatedInstances = [
				{
					[translations.Adapter[this.userSystemLanguage]]: '--none--',
					[translations.Instance[this.userSystemLanguage]]: '',
					[translations.Version[this.userSystemLanguage]]: '',
					[translations.Status[this.userSystemLanguage]]: '',
				},
			];
		}
		await this.setStateChangedAsync(`adapterAndInstances.listDeactivatedInstances`, { val: JSON.stringify(this.listDeactivatedInstances), ack: true });
		await this.setStateChangedAsync(`adapterAndInstances.countDeactivatedInstances`, { val: this.countDeactivatedInstances, ack: true });

		// list error instances
		if (this.countErrorInstance === 0) {
			this.listErrorInstance = [
				{
					[translations.Adapter[this.userSystemLanguage]]: '--none--',
					[translations.Instance[this.userSystemLanguage]]: '',
					[translations.Mode[this.userSystemLanguage]]: '',
					[translations.Status[this.userSystemLanguage]]: '',
				},
			];
		}
		await this.setStateChangedAsync(`adapterAndInstances.listInstancesError`, { val: JSON.stringify(this.listErrorInstance), ack: true });
		await this.setStateChangedAsync(`adapterAndInstances.countInstancesError`, { val: this.countErrorInstance, ack: true });
	}

	/**
	 * @param {string} id
	 */
	async renewAdapterUpdateData(id) {
		const previousAdapterUpdatesCount = this.countAdapterUpdates;

		// Fetch and process adapter update data
		await this.getAdapterUpdateData(id);
		await this.createAdapterUpdateList();

		// Check and send update notification if required
		if (this.config.checkSendAdapterUpdateMsg && this.countAdapterUpdates > previousAdapterUpdatesCount) {
			await this.sendStateNotifications('AdapterUpdates', 'updateAdapter', null);
		}

		// Update instances with available adapter updates
		for (const instance of this.listInstanceRaw.values()) {
			if (this.adapterUpdatesJsonRaw.has(instance.Adapter)) {
				const adapterUpdate = this.adapterUpdatesJsonRaw.get(instance.Adapter);
				instance.updateAvailable = adapterUpdate.newVersion;
			} else {
				instance.updateAvailable = ' - ';
			}
		}
	}
	/**
	 * call function on state change, renew data and send messages
	 * @param {string} id
	 * @param {ioBroker.State} state
	 */
	async renewInstanceData(id, state) {
		const instanceID = await this.getInstanceName(id);
		const instanceData = this.listInstanceRaw.get(instanceID);
		if (instanceData) {
			let instanceStatusRaw;

			const checkInstance = async (instanceID, instanceData) => {
				if (!instanceData.checkIsRunning) {
					instanceData.checkIsRunning = true;
					instanceStatusRaw = await this.setInstanceStatus(instanceData.instanceMode, instanceData.schedule, instanceID);
					instanceData.isAlive = instanceStatusRaw[0];
					instanceData.isHealthy = instanceStatusRaw[1];
					instanceData.status = instanceStatusRaw[2];
					instanceData.checkIsRunning = false;
					return;
				}
			};

			switch (id) {
				case `system.adapter.${instanceID}.alive`:
					if (state.val !== instanceData.isAlive) {
						await checkInstance(instanceID, instanceData);
						// send message when instance was deactivated
						if (this.config.checkSendInstanceDeactivatedMsg && !instanceData.isAlive) {
							if (this.blacklistInstancesNotify.includes(instanceID)) return;
							await this.sendStateNotifications('Instances', 'deactivatedInstance', instanceID);
						}
					}
					break;

				case `system.adapter.${instanceID}.connected`:
					if (state.val !== instanceData.isConnectedHost && instanceData.isAlive) {
						await checkInstance(instanceID, instanceData);
						// send message when instance has an error
						if (this.config.checkSendInstanceFailedMsg && !instanceData.isHealthy && instanceData.isAlive) {
							if (this.blacklistInstancesNotify.includes(instanceID)) return;
							await this.sendStateNotifications('Instances', 'errorInstance', instanceID);
						}
					}
					break;

				case `${instanceID}.info.connection`:
					if (state.val !== instanceData.isConnectedDevice && instanceData.isAlive) {
						await checkInstance(instanceID, instanceData);
						// send message when instance has an error
						if (this.config.checkSendInstanceFailedMsg && !instanceData.isHealthy && instanceData.isAlive) {
							if (this.blacklistInstancesNotify.includes(instanceID)) return;
							await this.sendStateNotifications('Instances', 'errorInstance', instanceID);
						}
					}
					break;
			}
		}
	}

	/**
	 * create Datapoints for Instances
	 */
	async createDPsForInstances() {
		await this.setObjectNotExistsAsync(`adapterAndInstances`, {
			type: 'channel',
			common: {
				name: {
					en: 'Adapter and Instances',
					de: 'Adapter und Instanzen',
					ru: 'Адаптер и Instances',
					pt: 'Adaptador e instâncias',
					nl: 'Adapter en Instance',
					fr: 'Adaptateur et instances',
					it: 'Adattatore e istanze',
					es: 'Adaptador e instalaciones',
					pl: 'Adapter and Instances',
					// @ts-ignore
					uk: 'Адаптер та інстанції',
					'zh-cn': '道歉和案',
				},
			},
			native: {},
		});

		// Instances
		await this.setObjectNotExistsAsync(`adapterAndInstances.listAllInstances`, {
			type: 'state',
			common: {
				name: {
					en: 'JSON List of all instances',
					de: 'JSON Liste aller Instanzen',
					ru: 'ДЖСОН Список всех инстанций',
					pt: 'J. Lista de todas as instâncias',
					nl: 'JSON List van alle instanties',
					fr: 'JSON Liste de tous les cas',
					it: 'JSON Elenco di tutte le istanze',
					es: 'JSON Lista de todos los casos',
					pl: 'JSON Lista wszystkich instancji',
					// @ts-ignore
					uk: 'Сонце Список всіх екземплярів',
					'zh-cn': '附 件 所有事例一览表',
				},
				type: 'array',
				role: 'json',
				read: true,
				write: false,
			},
			native: {},
		});
		await this.setObjectNotExistsAsync(`adapterAndInstances.countAllInstances`, {
			type: 'state',
			common: {
				name: {
					en: 'Number of all instances',
					de: 'Anzahl aller Instanzen',
					ru: 'Количество всех инстанций',
					pt: 'Número de todas as instâncias',
					nl: 'Nummer van alle gevallen',
					fr: 'Nombre de cas',
					it: 'Numero di tutte le istanze',
					es: 'Número de casos',
					pl: 'Liczba wszystkich instancji',
					// @ts-ignore
					uk: 'Кількість всіх екземплярів',
					'zh-cn': '各类案件数目',
				},
				type: 'number',
				role: 'value',
				read: true,
				write: false,
			},
			native: {},
		});
		// Instances
		await this.setObjectNotExistsAsync(`adapterAndInstances.listAllActiveInstances`, {
			type: 'state',
			common: {
				name: {
					en: 'JSON List of all active instances',
					de: 'JSON Liste aller aktiven Instanzen',
					ru: 'ДЖСОН Список всех активных инстанций',
					pt: 'J. Lista de todas as instâncias ativas',
					nl: 'JSON List van alle actieve instanties',
					fr: 'JSON Liste de tous les cas actifs',
					it: 'JSON Elenco di tutte le istanze attive',
					es: 'JSON Lista de todos los casos activos',
					pl: 'JSON Lista wszystkich aktywnych instancji',
					// @ts-ignore
					uk: 'Сонце Список всіх активних екземплярів',
					'zh-cn': '附 件 所有积极事件清单',
				},
				type: 'array',
				role: 'json',
				read: true,
				write: false,
			},
			native: {},
		});
		await this.setObjectNotExistsAsync(`adapterAndInstances.countAllActiveInstances`, {
			type: 'state',
			common: {
				name: {
					en: 'Number of all active instances',
					de: 'Anzahl aller aktiven Instanzen',
					ru: 'Количество всех активных инстанций',
					pt: 'Número de todas as instâncias ativas',
					nl: 'Nummer van alle actieve instanties',
					fr: 'Nombre de toutes les instances actives',
					it: 'Numero di tutte le istanze attive',
					es: 'Número de casos activos',
					pl: 'Liczba wszystkich czynnych przypadków',
					// @ts-ignore
					uk: 'Кількість всіх активних екземплярів',
					'zh-cn': '所有积极事件的数目',
				},
				type: 'number',
				role: 'value',
				read: true,
				write: false,
			},
			native: {},
		});
		await this.setObjectNotExistsAsync(`adapterAndInstances.listDeactivatedInstances`, {
			type: 'state',
			common: {
				name: {
					en: 'JSON List of deactivated instances',
					de: 'JSON Liste der deaktivierten Instanzen',
					ru: 'ДЖСОН Список деактивированных инстанций',
					pt: 'J. Lista de instâncias desativadas',
					nl: 'JSON List van gedeactiveerde instanties',
					fr: 'JSON Liste des cas désactivés',
					it: 'JSON Elenco delle istanze disattivate',
					es: 'JSON Lista de casos desactivados',
					pl: 'JSON Lista przypadków deaktywowanych',
					// @ts-ignore
					uk: 'Сонце Перелік деактивованих екземплярів',
					'zh-cn': '附 件 被动事例清单',
				},
				type: 'array',
				role: 'json',
				read: true,
				write: false,
			},
			native: {},
		});
		await this.setObjectNotExistsAsync(`adapterAndInstances.countDeactivatedInstances`, {
			type: 'state',
			common: {
				name: {
					en: 'Number of deactivated instances',
					de: 'Anzahl deaktivierter Instanzen',
					ru: 'Количество деактивированных инстанций',
					pt: 'Número de instâncias desativadas',
					nl: 'Nummer van gedeactiveerde instanties',
					fr: 'Nombre de cas désactivés',
					it: 'Numero di istanze disattivate',
					es: 'Número de casos desactivados',
					pl: 'Liczba deaktywowanych instancji',
					// @ts-ignore
					uk: 'Кількість деактивованих екземплярів',
					'zh-cn': 'A. 递解事件的数目',
				},
				type: 'number',
				role: 'value',
				read: true,
				write: false,
			},
			native: {},
		});
		await this.setObjectNotExistsAsync(`adapterAndInstances.listInstancesError`, {
			type: 'state',
			common: {
				name: {
					en: 'JSON list of instances with error',
					de: 'JSON-Liste von Instanzen mit Fehler',
					ru: 'JSON список инстанций с ошибкой',
					pt: 'Lista de instâncias JSON com erro',
					nl: 'JSON lijst met fouten',
					fr: 'Liste des instances avec erreur',
					it: 'Elenco JSON delle istanze con errore',
					es: 'JSON lista de casos con error',
					pl: 'Lista błędów JSON',
					// @ts-ignore
					uk: 'JSON список екземплярів з помилкою',
					'zh-cn': '联合工作组办公室错误事件清单',
				},
				type: 'array',
				role: 'json',
				read: true,
				write: false,
			},
			native: {},
		});
		await this.setObjectNotExistsAsync(`adapterAndInstances.countInstancesError`, {
			type: 'state',
			common: {
				name: {
					en: 'Count of instances with error',
					de: 'Anzahl der Instanzen mit Fehler',
					ru: 'Количество инстанций с ошибкой',
					pt: 'Contagem de instâncias com erro',
					nl: 'Graaf van instoringen met fouten',
					fr: 'Nombre de cas avec erreur',
					it: 'Conteggio di istanze con errore',
					es: 'Cuenta de casos con error',
					pl: 'Liczba przykładów w przypadku błędów',
					// @ts-ignore
					uk: 'Кількість екземплярів з помилкою',
					'zh-cn': '发生错误的情况',
				},
				type: 'number',
				role: 'value',
				read: true,
				write: false,
			},
			native: {},
		});

		// Adapter
		await this.setObjectNotExistsAsync(`adapterAndInstances.listAdapterUpdates`, {
			type: 'state',
			common: {
				name: {
					en: 'JSON list of adapters with available updates',
					de: 'JSON-Liste der Adapter mit verfügbaren Updates',
					ru: 'JSON список адаптеров с доступными обновлениями',
					pt: 'Lista de adaptadores JSON com atualizações disponíveis',
					nl: 'JSON lijst met beschikbare updates',
					fr: 'Liste JSON des adaptateurs avec mises à jour disponibles',
					it: 'Elenco di adattatori JSON con aggiornamenti disponibili',
					es: 'JSON lista de adaptadores con actualizaciones disponibles',
					pl: 'JSON lista adapterów z dostępnymi aktualizacjami',
					// @ts-ignore
					uk: 'JSON список адаптерів з доступними оновленнями',
					'zh-cn': '附录A',
				},
				type: 'array',
				role: 'json',
				read: true,
				write: false,
			},
			native: {},
		});
		await this.setObjectNotExistsAsync(`adapterAndInstances.countAdapterUpdates`, {
			type: 'state',
			common: {
				name: {
					en: 'Number of adapters with available updates',
					de: 'Anzahl der Adapter mit verfügbaren Updates',
					ru: 'Количество адаптеров с доступными обновлениями',
					pt: 'Número de adaptadores com atualizações disponíveis',
					nl: 'Nummer van adapters met beschikbare updates',
					fr: "Nombre d'adaptateurs avec mises à jour disponibles",
					it: 'Numero di adattatori con aggiornamenti disponibili',
					es: 'Número de adaptadores con actualizaciones disponibles',
					pl: 'Liczba adapterów z dostępną aktualizacją',
					// @ts-ignore
					uk: 'Кількість адаптерів з доступними оновленнями',
					'zh-cn': '更新的适应者人数',
				},
				type: 'number',
				role: 'value',
				read: true,
				write: false,
			},
			native: {},
		});
	}

	/**
	 * delete Datapoints for Instances
	 */
	async deleteDPsForInstances() {
		await this.delObjectAsync(`adapterAndInstances`);
		await this.delObjectAsync(`adapterAndInstances.listAllInstances`);
		await this.delObjectAsync(`adapterAndInstances.countAllInstances`);
		await this.delObjectAsync(`adapterAndInstances.listAllActiveInstances`);
		await this.delObjectAsync(`adapterAndInstances.countAllActiveInstances`);
		await this.delObjectAsync(`adapterAndInstances.listDeactivatedInstances`);
		await this.delObjectAsync(`adapterAndInstances.countDeactivatedInstances`);
		await this.delObjectAsync(`adapterAndInstances.listInstancesError`);
		await this.delObjectAsync(`adapterAndInstances.countInstancesError`);
		await this.delObjectAsync(`adapterAndInstances.listAdapterUpdates`);
		await this.delObjectAsync(`adapterAndInstances.countAdapterUpdates`);
	}

	/*=============================================
	=       functions to send notifications       =
	=============================================*/

	/**
	 * Notification service
	 * @param {string} text - Text which should be send
	 */
	async sendNotification(text) {
		// Pushover
		if (this.config.instancePushover) {
			try {
				//first check if instance is living
				const pushoverAliveState = await this.getInitValue('system.adapter.' + this.config.instancePushover + '.alive');

				if (!pushoverAliveState) {
					this.log.warn('Pushover instance is not running. Message could not be sent. Please check your instance configuration.');
				} else {
					await this.sendToAsync(this.config.instancePushover, 'send', {
						message: text,
						title: this.config.titlePushover,
						device: this.config.devicePushover,
						user: this.config.userPushover,
						priority: this.config.prioPushover,
					});
				}
			} catch (error) {
				this.log.error(`[sendNotification Pushover] - ${error}`);
			}
		}

		// Telegram
		if (this.config.instanceTelegram) {
			try {
				//first check if instance is living
				const telegramAliveState = await this.getInitValue('system.adapter.' + this.config.instanceTelegram + '.alive');

				if (!telegramAliveState) {
					this.log.warn('Telegram instance is not running. Message could not be sent. Please check your instance configuration.');
				} else {
					await this.sendToAsync(this.config.instanceTelegram, 'send', {
						text: text,
						user: this.config.deviceTelegram,
						chatId: this.config.chatIdTelegram,
					});
				}
			} catch (error) {
				this.log.error(`[sendNotification Telegram] - ${error}`);
			}
		}

		// Whatsapp
		if (this.config.instanceWhatsapp) {
			try {
				//first check if instance is living
				const whatsappAliveState = await this.getInitValue('system.adapter.' + this.config.instanceWhatsapp + '.alive');

				if (!whatsappAliveState) {
					this.log.warn('Whatsapp instance is not running. Message could not be sent. Please check your instance configuration.');
				} else {
					await this.sendToAsync(this.config.instanceWhatsapp, 'send', {
						text: text,
						phone: this.config.phoneWhatsapp,
					});
				}
			} catch (error) {
				this.log.error(`[sendNotification Whatsapp] - ${error}`);
			}
		}

		// Matrix
		if (this.config.instanceMatrix) {
			try {
				//first check if instance is living
				const matrixAliveState = await this.getInitValue('system.adapter.' + this.config.instanceMatrix + '.alive');

				if (!matrixAliveState) {
					this.log.warn('Matrix instance is not running. Message could not be sent. Please check your instance configuration.');
				} else {
					await this.sendToAsync(this.config.instanceMatrix, 'send', {
						html: `<h1>${this.config.titleMatrix}</h1>`,
						text: text,
					});
				}
			} catch (error) {
				this.log.error(`[sendNotification Matrix] - ${error}`);
			}
		}

		// Signal
		if (this.config.instanceSignal) {
			try {
				//first check if instance is living
				const signalAliveState = await this.getInitValue('system.adapter.' + this.config.instanceSignal + '.alive');

				if (!signalAliveState) {
					this.log.warn('Signal instance is not running. Message could not be sent. Please check your instance configuration.');
				} else {
					await this.sendToAsync(this.config.instanceSignal, 'send', {
						text: text,
						phone: this.config.phoneSignal,
					});
				}
			} catch (error) {
				this.log.error(`[sendNotification Signal] - ${error}`);
			}
		}

		// Email
		if (this.config.instanceEmail) {
			try {
				//first check if instance is living
				const eMailAliveState = await this.getInitValue('system.adapter.' + this.config.instanceEmail + '.alive');

				if (!eMailAliveState) {
					this.log.warn('eMail instance is not running. Message could not be sent. Please check your instance configuration.');
				} else {
					await this.sendToAsync(this.config.instanceEmail, 'send', {
						sendTo: this.config.sendToEmail,
						text: text,
						subject: this.config.subjectEmail,
					});
				}
			} catch (error) {
				this.log.error(`[sendNotification eMail] - ${error}`);
			}
		}

		// Jarvis Notification
		if (this.config.instanceJarvis) {
			try {
				//first check if instance is living
				const jarvisAliveState = await this.getInitValue('system.adapter.' + this.config.instanceJarvis + '.alive');

				if (!jarvisAliveState) {
					this.log.warn('Jarvis instance is not running. Message could not be sent. Please check your instance configuration.');
				} else {
					const jsonText = JSON.stringify(text);
					await this.setForeignStateAsync(
						`${this.config.instanceJarvis}.addNotification`,
						'{"title":"' + this.config.titleJarvis + ' (' + this.formatDate(new Date(), 'DD.MM.YYYY - hh:mm:ss') + ')","message": ' + jsonText + ',"display": "drawer"}',
					);
				}
			} catch (error) {
				this.log.error(`[sendNotification Jarvis] - ${error}`);
			}
		}

		// Lovelace Notification
		if (this.config.instanceLovelace) {
			try {
				//first check if instance is living
				const lovelaceAliveState = await this.getInitValue('system.adapter.' + this.config.instanceLovelace + '.alive');

				if (!lovelaceAliveState) {
					this.log.warn('Lovelace instance is not running. Message could not be sent. Please check your instance configuration.');
				} else {
					const jsonText = JSON.stringify(text);
					await this.setForeignStateAsync(
						`${this.config.instanceLovelace}.notifications.add`,
						'{"message":' + jsonText + ', "title":"' + this.config.titleLovelace + ' (' + this.formatDate(new Date(), 'DD.MM.YYYY - hh:mm:ss') + ')"}',
					);
				}
			} catch (error) {
				this.log.error(`[sendNotification Lovelace] - ${error}`);
			}
		}

		// Synochat Notification
		if (this.config.instanceSynochat) {
			try {
				//first check if instance is living
				const synochatAliveState = await this.getInitValue('system.adapter.' + this.config.instanceSynochat + '.alive');

				if (!synochatAliveState) {
					this.log.warn('Synochat instance is not running. Message could not be sent. Please check your instance configuration.');
				} else {
					if (this.config.channelSynochat !== undefined) {
						await this.setForeignStateAsync(`${this.config.instanceSynochat}.${this.config.channelSynochat}.message`, text);
					} else {
						this.log.warn('Synochat channel is not set. Message could not be sent. Please check your instance configuration.');
					}
				}
			} catch (error) {
				this.log.error(`[sendNotification Synochat] - ${error}`);
			}
		}
	} // <-- End of sendNotification function

	/*---------- Notifications ----------*/
	/**
	 * Notifications on state changes
	 * @param {string} mainType
	 * @param {string} type
	 * @param {object} id
	 */
	async sendStateNotifications(mainType, type, id) {
		if (isUnloaded) return;
		let objectData;
		let adapterName;
		let list = '';
		let message = '';

		if (id !== null) {
			if (mainType === 'Devices') {
				objectData = this.listAllDevicesRaw.get(id);
				adapterName = this.config.showAdapterNameinMsg ? `${objectData.Adapter}: ` : '';
			} else if (mainType === 'Instances') {
				objectData = this.listInstanceRaw.get(id);
			}
		}

		const setMessage = async (message) => {
			this.log.info(message);
			await this.setStateAsync('lastNotification', message, true);
			await this.sendNotification(message);
		};

		switch (type) {
			case 'lowBatDevice':
				message = `${translations.Device_low_bat_detected[this.userSystemLanguage]}: \n${adapterName} ${objectData.Device} (${objectData.Battery})`;
				await setMessage(message);
				break;

			case 'onlineStateDevice':
				switch (objectData.Status) {
					case 'Online':
						message = `${translations.Device_available_again[this.userSystemLanguage]}: \n${adapterName} ${objectData.Device} (${objectData.LastContact})`;
						break;

					case 'Offline':
						message = `${translations.Device_not_reachable[this.userSystemLanguage]}: \n${adapterName} ${objectData.Device} (${objectData.LastContact})`;
						break;
				}
				await setMessage(message);
				break;

			case 'updateDevice':
				message = `${translations.Device_new_updates[this.userSystemLanguage]}: \n${adapterName} ${objectData.Device}`;
				await setMessage(message);
				break;

			case 'updateAdapter':
				if (this.countAdapterUpdates === 0) return;

				objectData = this.listAdapterUpdates;
				list = '';

				for (const id of objectData) {
					list = `${list}\n${id[translations.Adapter[this.userSystemLanguage]]}: v${id[translations.Available_Version[this.userSystemLanguage]]}`;
				}

				message = `${translations.Adapter_new_updates[this.userSystemLanguage]}: ${list}`;
				await setMessage(message);
				break;

			case 'errorInstance':
			case 'deactivatedInstance':
				message = `${translations.Instance_Watchdog[this.userSystemLanguage]}:\n${id}: ${objectData.status}`;
				await setMessage(message);
				break;
		}
	}

	/**
	 * Notifications per user defined schedule
	 * @param {string} type
	 */
	async sendScheduleNotifications(type) {
		if (isUnloaded) return;

		const checkDays = [];
		const dayConfigKeys = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
		let list = '';
		let message = '';

		const setMessage = async (message) => {
			this.log.info(message);
			await this.setStateAsync('lastNotification', message, true);
			await this.sendNotification(message);
		};

		const processDeviceList = (deviceList, property1, property2) => {
			for (const id of deviceList) {
				if (this.blacklistNotify.includes(id.Path)) continue;
				list += `\n${!this.config.showAdapterNameinMsg ? '' : id.Adapter + ': '}${id[property1]}${property2 ? ` (${id[property2]})` : ''}`;
			}
		};

		const processInstanceList = (instanceList, property) => {
			for (const id of instanceList) {
				if (this.blacklistInstancesNotify.includes(id[translations['Instance'][this.userSystemLanguage]])) continue;
				list += `\n${id[translations['Instance'][this.userSystemLanguage]]}${property ? `: ${id[property]}` : ''}`;
			}
		};

		const processNotification = async (list, messageType) => {
			if (list.length === 0) return;

			switch (checkDays.length) {
				case 1:
					message = `${translations.Weekly_overview[this.userSystemLanguage]} ${translations[messageType][this.userSystemLanguage]}: ${list}`;
					break;
				case 7:
					message = `${translations.Daily_overview[this.userSystemLanguage]} ${translations[messageType][this.userSystemLanguage]}: ${list}`;
					break;
				default:
					message = `${translations.Overview_of[this.userSystemLanguage]} ${translations[messageType][this.userSystemLanguage]}: ${list}`;
					break;
			}

			await setMessage(message);
		};

		switch (type) {
			case 'lowBatteryDevices':
				checkDays.push(...dayConfigKeys.map((day, index) => (this.config['check' + day] ? index : null)).filter((day) => day !== null));

				if (checkDays.length === 0) {
					this.log.warn(`No days selected for daily low battery devices message. Please check the instance configuration!`);
					return;
				}
				this.log.debug(`Number of selected days for daily low battery devices message: ${checkDays.length}. Send Message on: ${checkDays.join(', ')} ...`);

				schedule.scheduleJob(`1 ${this.config.checkSendBatteryTime.split(':').reverse().join(' ')} * * ${checkDays.join(',')}`, async () => {
					processDeviceList(this.batteryLowPoweredRaw, 'Device', 'Battery');

					await processNotification(list, 'devices_low_bat');
				});
				break;

			case 'offlineDevices':
				checkDays.push(...dayConfigKeys.map((day, index) => (this.config['checkOffline' + day] ? index : null)).filter((day) => day !== null));

				if (checkDays.length === 0) {
					this.log.warn(`No days selected for daily offline devices message. Please check the instance configuration!`);
					return;
				}
				this.log.debug(`Number of selected days for daily offline devices message: ${checkDays.length}. Send Message on: ${checkDays.join(', ')} ...`);

				schedule.scheduleJob(`2 ${this.config.checkSendOfflineTime.split(':').reverse().join(' ')} * * ${checkDays.join(',')}`, async () => {
					processDeviceList(this.offlineDevicesRaw, `Device`, 'LastContact');

					await processNotification(list, 'offline_devices');
				});
				break;

			case 'updateDevices':
				checkDays.push(...dayConfigKeys.map((day, index) => (this.config['checkUpgrade' + day] ? index : null)).filter((day) => day !== null));

				if (checkDays.length === 0) {
					this.log.warn(`No days selected for daily updatable devices message. Please check the instance configuration!`);
					return;
				}
				this.log.debug(`Number of selected days for daily updatable devices message: ${checkDays.length}. Send Message on: ${checkDays.join(', ')} ...`);

				schedule.scheduleJob(`3 ${this.config.checkSendUpgradeTime.split(':').reverse().join(' ')} * * ${checkDays.join(',')}`, async () => {
					processDeviceList(this.upgradableDevicesRaw, 'Device');

					await processNotification(list, 'available_updatable_devices');
				});
				break;

			case 'updateAdapter':
				checkDays.push(...dayConfigKeys.map((day, index) => (this.config['checkAdapterUpdate' + day] ? index : null)).filter((day) => day !== null));

				if (checkDays.length === 0) {
					this.log.warn(`No days selected for daily adapter update message. Please check the instance configuration!`);
					return;
				}
				this.log.debug(`Number of selected days for daily adapter update message: ${checkDays.length}. Send Message on: ${checkDays.join(', ')} ...`);

				schedule.scheduleJob(`4 ${this.config.checkSendAdapterUpdateTime.split(':').reverse().join(' ')} * * ${checkDays.join(',')}`, async () => {
					for (const id of this.listAdapterUpdates) {
						list = `${list}\n${id[translations.Adapter[this.userSystemLanguage]]}: v${id[translations.Available_Version[this.userSystemLanguage]]}`;
					}
					await processNotification(list, 'available_adapter_updates');
				});
				break;

			case 'errorInstance':
				checkDays.push(...dayConfigKeys.map((day, index) => (this.config['checkFailedInstances' + day] ? index : null)).filter((day) => day !== null));

				if (checkDays.length === 0) {
					this.log.warn(`No days selected for daily instance error message. Please check the instance configuration!`);
					return;
				}
				this.log.debug(`Number of selected days for daily instance error message: ${checkDays.length}. Send Message on: ${checkDays.join(', ')} ...`);

				schedule.scheduleJob(`5 ${this.config.checkSendInstanceFailedTime.split(':').reverse().join(' ')} * * ${checkDays.join(',')}`, async () => {
					processInstanceList(this.listErrorInstanceRaw, 'Status');

					await processNotification(list, 'error_instances_msg');
				});
				break;

			case 'deactivatedInstance':
				checkDays.push(...dayConfigKeys.map((day, index) => (this.config['checkInstanceDeactivated' + day] ? index : null)).filter((day) => day !== null));

				if (checkDays.length === 0) {
					this.log.warn(`No days selected for daily instance deactivated message. Please check the instance configuration!`);
					return;
				}
				this.log.debug(`Number of selected days for daily instance deactivated message: ${checkDays.length}. Send Message on: ${checkDays.join(', ')} ...`);

				schedule.scheduleJob(`5 ${this.config.checkSendInstanceDeactivatedTime.split(':').reverse().join(' ')} * * ${checkDays.join(',')}`, async () => {
					processInstanceList(this.listDeactivatedInstances);

					await processNotification(list, 'deactivated_instances_msg');
				});
				break;
		}
	}

	/*=============================================
	=       functions to create html lists        =
	=============================================*/
	/**
	 * @param {string} type - type of list
	 * @param {object} devices - Device
	 * @param {number} deviceCount - Counted devices
	 * @param {object} isLowBatteryList - list Low Battery Devices
	 */
	async createListHTML(type, devices, deviceCount, isLowBatteryList) {
		let html;
		switch (type) {
			case 'linkQualityList':
				devices = devices.sort((a, b) => {
					a = a.Device || '';
					b = b.Device || '';
					return a.localeCompare(b);
				});
				html = `<center>
			<b>${[translations.Link_quality_devices[this.userSystemLanguage]]}:<font> ${deviceCount}</b><small></small></font>
			<p></p>
			</center>   
			<table width=100%>
			<tr>
			<th align=left>${[translations.Device[this.userSystemLanguage]]}</th>
			<th align=center width=120>${[translations.Adapter[this.userSystemLanguage]]}</th>
			<th align=right>${[translations.Signal_strength[this.userSystemLanguage]]}</th>
			</tr>
			<tr>
			<td colspan="5"><hr></td>
			</tr>`;

				for (const device of devices) {
					html += `<tr>
				<td><font>${device[translations.Device[this.userSystemLanguage]]}</font></td>
				<td align=center><font>${device[translations.Adapter[this.userSystemLanguage]]}</font></td>
				<td align=right><font>${device[translations.Signal_strength[this.userSystemLanguage]]}</font></td>
				</tr>`;
				}

				html += '</table>';
				break;

			case 'offlineList':
				devices = devices.sort((a, b) => {
					a = a.Device || '';
					b = b.Device || '';
					return a.localeCompare(b);
				});
				html = `<center>
			<b>${[translations.offline_devices[this.userSystemLanguage]]}: <font color=${deviceCount === 0 ? '#3bcf0e' : 'orange'}>${deviceCount}</b><small></small></font>
			<p></p>
			</center>   
			<table width=100%>
			<tr>
			<th align=left>${[translations.Device[this.userSystemLanguage]]}</th>
			<th align=center width=120>${[translations.Adapter[this.userSystemLanguage]]}</th>
			<th align=center>${[translations.Last_Contact[this.userSystemLanguage]]}</th>
			</tr>
			<tr>
			<td colspan="5"><hr></td>
			</tr>`;

				for (const device of devices) {
					html += `<tr>
				<td><font>${device[translations.Device[this.userSystemLanguage]]}</font></td>
				<td align=center><font>${device[translations.Adapter[this.userSystemLanguage]]}</font></td>
				<td align=center><font color=orange>${device[translations.Last_Contact[this.userSystemLanguage]]}</font></td>
				</tr>`;
				}

				html += '</table>';
				break;

			case 'batteryList':
				devices = devices.sort((a, b) => {
					a = a.Device || '';
					b = b.Device || '';
					return a.localeCompare(b);
				});
				html = `<center>
			<b>${isLowBatteryList === true ? `${[translations.low[this.userSystemLanguage]]} ` : ''}${[translations.battery_devices[this.userSystemLanguage]]}: 
			<font color=${isLowBatteryList === true ? (deviceCount > 0 ? 'orange' : '#3bcf0e') : ''}>${deviceCount}</b></font>
			<p></p>
			</center>   
			<table width=100%>
			<tr>
			<th align=left>${[translations.Device[this.userSystemLanguage]]}</th>
			<th align=center width=120>${[translations.Adapter[this.userSystemLanguage]]}</th>
			<th align=${isLowBatteryList ? 'center' : 'right'}>${[translations.Battery[this.userSystemLanguage]]}</th>
			</tr>
			<tr>
			<td colspan="5"><hr></td>
			</tr>`;
				for (const device of devices) {
					html += `<tr>
				<td><font>${device[translations.Device[this.userSystemLanguage]]}</font></td>
				<td align=center><font>${device[translations.Adapter[this.userSystemLanguage]]}</font></td>`;

					if (isLowBatteryList) {
						html += `<td align=center><font color=orange>${device[translations.Battery[this.userSystemLanguage]]}</font></td>`;
					} else {
						html += `<td align=right><font color=#3bcf0e>${device[translations.Battery[this.userSystemLanguage]]}</font></td>`;
					}
					html += `</tr>`;
				}

				html += '</table>';
				break;
		}
		return html;
	}

	/**
	 * @param {string} type - type of list
	 * @param {object} instances - Instance
	 * @param {number} instancesCount - Counted devices
	 */
	async createListHTMLInstances(type, instances, instancesCount) {
		let html;
		switch (type) {
			case 'allInstancesList':
				instances = instances.sort((a, b) => {
					a = a.Instance || '';
					b = b.Instance || '';
					return a.localeCompare(b);
				});
				html = `<center>
				<b>${[translations.All_Instances[this.userSystemLanguage]]}:<font> ${instancesCount}</b><small></small></font>
				<p></p>
				</center>   
				<table width=100%>
				<tr>
				<th align=left>${[translations.Adapter[this.userSystemLanguage]]}</th>
				<th align=center>${[translations.Instance[this.userSystemLanguage]]}</th>
				<th align=center width=180>${[translations.Status[this.userSystemLanguage]]}</th>
				</tr>
				<tr>
				<td colspan="5"><hr></td>
				</tr>`;

				for (const instanceData of instances) {
					html += `<tr>
					<td><font>${instanceData[translations.Adapter[this.userSystemLanguage]]}</font></td>
					<td align=center><font>${instanceData[translations.Instance[this.userSystemLanguage]]}</font></td>
					<td align=center><font>${instanceData[translations.Status[this.userSystemLanguage]]}</font></td>
					</tr>`;
				}

				html += '</table>';
				break;

			case 'allActiveInstancesList':
				instances = instances.sort((a, b) => {
					a = a.Instance || '';
					b = b.Instances || '';
					return a.localeCompare(b);
				});
				html = `<center>
				<b>${[translations.Active_Instances[this.userSystemLanguage]]}: <font> ${instancesCount}</b><small></small></font>
				<p></p>
				</center>   
				<table width=100%>
				<tr>
				<th align=left>${[translations.Adapter[this.userSystemLanguage]]}</th>
				<th align=center>${[translations.Instance[this.userSystemLanguage]]}</th>
				<th align=center width=180>${[translations.Status[this.userSystemLanguage]]}</th>
				</tr>
				<tr>
				<td colspan="5"><hr></td>
				</tr>`;

				for (const instanceData of instances) {
					html += `<tr>
					<td><font>${instanceData[translations.Adapter[this.userSystemLanguage]]}</font></td>
					<td align=center><font>${instanceData[translations.Instance[this.userSystemLanguage]]}</font></td>
					<td align=center><font color=orange>${instanceData[translations.Status[this.userSystemLanguage]]}</font></td>
					</tr>`;
				}

				html += '</table>';
				break;

			case 'errorInstanceList':
				instances = instances.sort((a, b) => {
					a = a.Instance || '';
					b = b.Instances || '';
					return a.localeCompare(b);
				});
				html = `<center>
				<b>${[translations.Error_Instances[this.userSystemLanguage]]}: <font color=${instancesCount === 0 ? '#3bcf0e' : 'orange'}>${instancesCount}</b><small></small></font>
				<p></p>
				</center>   
				<table width=100%>
				<tr>
				<th align=left>${[translations.Adapter[this.userSystemLanguage]]}</th>
				<th align=center>${[translations.Instance[this.userSystemLanguage]]}</th>
				<th align=center width=180>${[translations.Status[this.userSystemLanguage]]}</th>
				</tr>
				<tr>
				<td colspan="5"><hr></td>
				</tr>`;

				for (const instanceData of instances) {
					html += `<tr>
					<td><font>${instanceData[translations.Adapter[this.userSystemLanguage]]}</font></td>
					<td align=center><font>${instanceData[translations.Instance[this.userSystemLanguage]]}</font></td>
					<td align=center><font color=orange>${instanceData[translations.Status[this.userSystemLanguage]]}</font></td>
					</tr>`;
				}

				html += '</table>';
				break;

			case 'deactivatedInstanceList':
				instances = instances.sort((a, b) => {
					a = a.Instance || '';
					b = b.Instances || '';
					return a.localeCompare(b);
				});
				html = `<center>
				<b>${[translations.Deactivated_Instances[this.userSystemLanguage]]}: <font color=${instancesCount === 0 ? '#3bcf0e' : 'orange'}>${instancesCount}</b><small></small></font>
				<p></p>
				</center>   
				<table width=100%>
				<tr>
				<th align=left>${[translations.Adapter[this.userSystemLanguage]]}</th>
				<th align=center>${[translations.Instance[this.userSystemLanguage]]}</th>
				<th align=center width=180>${[translations.Status[this.userSystemLanguage]]}</th>
				</tr>
				<tr>
				<td colspan="5"><hr></td>
				</tr>`;

				for (const instanceData of instances) {
					if (!instanceData.isAlive) {
						html += `<tr>
					<td><font>${instanceData[translations.Adapter[this.userSystemLanguage]]}</font></td>
					<td align=center><font>${instanceData[translations.Instance[this.userSystemLanguage]]}</font></td>
					<td align=center><font color=orange>${instanceData[translations.Status[this.userSystemLanguage]]}</font></td>
					</tr>`;
					}
				}

				html += '</table>';
				break;

			case 'updateAdapterList':
				html = `<center>
				<b>${[translations.Updatable_adapters[this.userSystemLanguage]]}: <font color=${instancesCount === 0 ? '#3bcf0e' : 'orange'}>${instancesCount}</b><small></small></font>
				<p></p>
				</center>   
				<table width=100%>
				<tr>
				<th align=left>${[translations.Adapter[this.userSystemLanguage]]}</th>
				<th align=center>${[translations.Installed_Version[this.userSystemLanguage]]}</th>
				<th align=center>${[translations.Available_Version[this.userSystemLanguage]]}</th>
				</tr>
				<tr>
				<td colspan="5"><hr></td>
				</tr>`;

				for (const instanceData of instances.values()) {
					if (instanceData.updateAvailable !== ' - ') {
						html += `<tr>
					<td><font>${instanceData[translations.Adapter[this.userSystemLanguage]]}</font></td>
					<td align=center><font>${instanceData[translations.Installed_Version[this.userSystemLanguage]]}</font></td>
					<td align=center><font color=orange>${instanceData[translations.Available_Version[this.userSystemLanguage]]}</font></td>
					</tr>`;
					}
				}

				html += '</table>';
				break;
		}
		return html;
	}

	/*=============================================
	=     create datapoints for each adapter      =
	=============================================*/

	/**
	 * @param {object} adptName - Adaptername of devices
	 */
	async createDPsForEachAdapter(adptName) {
		await this.setObjectNotExistsAsync(`devices.${adptName}`, {
			type: 'channel',
			common: {
				name: adptName,
			},
			native: {},
		});

		await this.setObjectNotExistsAsync(`devices.${adptName}.offlineCount`, {
			type: 'state',
			common: {
				name: {
					en: 'Number of devices offline',
					de: 'Anzahl der Geräte offline',
					ru: 'Количество устройств offline',
					pt: 'Número de dispositivos offline',
					nl: 'Nummer van apparatuur offline',
					fr: 'Nombre de dispositifs hors ligne',
					it: 'Numero di dispositivi offline',
					es: 'Número de dispositivos sin conexión',
					pl: 'Ilość urządzeń offline',
					'zh-cn': '线内装置数量',
				},
				type: 'number',
				role: 'value',
				read: true,
				write: false,
			},
			native: {},
		});

		await this.setObjectNotExistsAsync(`devices.${adptName}.offlineList`, {
			type: 'state',
			common: {
				name: {
					en: 'List of offline devices',
					de: 'Liste der Offline-Geräte',
					ru: 'Список оффлайн устройств',
					pt: 'Lista de dispositivos off-line',
					nl: 'List van offline apparatuur',
					fr: 'Liste des dispositifs hors ligne',
					it: 'Elenco dei dispositivi offline',
					es: 'Lista de dispositivos sin conexión',
					pl: 'Lista urządzeń offline',
					'zh-cn': '线装置清单',
				},
				type: 'array',
				role: 'json',
				read: true,
				write: false,
			},
			native: {},
		});

		await this.setObjectNotExistsAsync(`devices.${adptName}.oneDeviceOffline`, {
			type: 'state',
			common: {
				name: {
					en: 'Is one device with offline',
					de: 'Ist ein Gerät mit Offline',
					ru: 'Это одно устройство с offline',
					pt: 'É um dispositivo com offline',
					nl: 'Is een apparaat met offline',
					fr: 'Est un appareil avec hors ligne',
					it: 'È un dispositivo con offline',
					es: 'Es un dispositivo sin conexión',
					pl: 'Jest to jeden urządzenie z offlinem',
					// @ts-ignore
					uk: 'Є один пристрій з автономним',
					'zh-cn': '一处有线装置',
				},
				type: 'boolean',
				role: 'state',
				read: true,
				write: false,
				def: false,
			},
			native: {},
		});

		await this.setObjectNotExistsAsync(`devices.${adptName}.listAllRawJSON`, {
			type: 'state',
			common: {
				name: {
					en: 'JSON RAW List of all devices',
					de: 'JSON RAW Liste aller Geräte',
					ru: 'ДЖСОН РАВ Список всех устройств',
					pt: 'JSON RAW Lista de todos os dispositivos',
					nl: 'JSON RAW List van alle apparaten',
					fr: 'JSON RAW Liste de tous les dispositifs',
					it: 'JSON RAW Elenco di tutti i dispositivi',
					es: 'JSON RAW Lista de todos los dispositivos',
					pl: 'JSON RAW Lista wszystkich urządzeń',
					// @ts-ignore
					uk: 'ДЖСОН РАВ Список всіх пристроїв',
					'zh-cn': 'JSONRAW 所有装置清单',
				},
				type: 'array',
				role: 'json',
				read: true,
				write: false,
			},
			native: {},
		});

		await this.setObjectNotExistsAsync(`devices.${adptName}.listAll`, {
			type: 'state',
			common: {
				name: {
					en: 'List of all devices',
					de: 'Liste aller Geräte',
					ru: 'Список всех устройств',
					pt: 'Lista de todos os dispositivos',
					nl: 'List van alle apparaten',
					fr: 'Liste de tous les dispositifs',
					it: 'Elenco di tutti i dispositivi',
					es: 'Lista de todos los dispositivos',
					pl: 'Lista wszystkich urządzeń',
					'zh-cn': '所有装置清单',
				},
				type: 'array',
				role: 'json',
				read: true,
				write: false,
			},
			native: {},
		});

		await this.setObjectNotExistsAsync(`devices.${adptName}.linkQualityList`, {
			type: 'state',
			common: {
				name: {
					en: 'List of devices with signal strength',
					de: 'Liste der Geräte mit Signalstärke',
					ru: 'Список устройств с силой сигнала',
					pt: 'Lista de dispositivos com força de sinal',
					nl: 'List van apparaten met signaalkracht',
					fr: 'Liste des dispositifs avec force de signal',
					it: 'Elenco dei dispositivi con forza del segnale',
					es: 'Lista de dispositivos con fuerza de señal',
					pl: 'Lista urządzeń z siłą sygnałową',
					'zh-cn': '具有信号实力的装置清单',
				},
				type: 'array',
				role: 'json',
				read: true,
				write: false,
			},
			native: {},
		});

		await this.setObjectNotExistsAsync(`devices.${adptName}.countAll`, {
			type: 'state',
			common: {
				name: {
					en: 'Number of all devices',
					de: 'Anzahl aller Geräte',
					ru: 'Количество всех устройств',
					pt: 'Número de todos os dispositivos',
					nl: 'Nummer van alle apparaten',
					fr: 'Nombre de tous les appareils',
					it: 'Numero di tutti i dispositivi',
					es: 'Número de todos los dispositivos',
					pl: 'Ilość wszystkich urządzeń',
					'zh-cn': '所有装置的数目',
				},
				type: 'number',
				role: 'value',
				read: true,
				write: false,
			},
			native: {},
		});

		await this.setObjectNotExistsAsync(`devices.${adptName}.batteryList`, {
			type: 'state',
			common: {
				name: {
					en: 'List of devices with battery state',
					de: 'Liste der Geräte mit Batteriezustand',
					ru: 'Список устройств с состоянием батареи',
					pt: 'Lista de dispositivos com estado da bateria',
					nl: 'List van apparaten met batterij staat',
					fr: 'Liste des appareils avec état de batterie',
					it: 'Elenco dei dispositivi con stato della batteria',
					es: 'Lista de dispositivos con estado de batería',
					pl: 'Lista urządzeń z baterią stanową',
					'zh-cn': '电池国装置清单',
				},
				type: 'array',
				role: 'json',
				read: true,
				write: false,
			},
			native: {},
		});

		await this.setObjectNotExistsAsync(`devices.${adptName}.lowBatteryList`, {
			type: 'state',
			common: {
				name: {
					en: 'List of devices with low battery state',
					de: 'Liste der Geräte mit niedrigem Batteriezustand',
					ru: 'Список устройств с низким состоянием батареи',
					pt: 'Lista de dispositivos com baixo estado da bateria',
					nl: 'List van apparaten met lage batterij staat',
					fr: 'Liste des appareils à faible état de batterie',
					it: 'Elenco di dispositivi con stato di batteria basso',
					es: 'Lista de dispositivos con estado de batería bajo',
					pl: 'Lista urządzeń o niskim stanie baterii',
					'zh-cn': '低电池国家装置清单',
				},
				type: 'array',
				role: 'json',
				read: true,
				write: false,
			},
			native: {},
		});

		await this.setObjectNotExistsAsync(`devices.${adptName}.lowBatteryCount`, {
			type: 'state',
			common: {
				name: {
					en: 'Number of devices with low battery',
					de: 'Anzahl der Geräte mit niedriger Batterie',
					ru: 'Количество устройств c низкой батареей',
					pt: 'Número de dispositivos com bateria baixa',
					nl: 'Nummer van apparaten met lage batterij',
					fr: 'Nombre de dispositifs avec batterie basse',
					it: 'Numero di dispositivi con batteria bassa',
					es: 'Número de dispositivos con batería baja',
					pl: 'Liczba urządzeń z niską baterią',
					'zh-cn': '低电池的装置数量',
				},
				type: 'number',
				role: 'value',
				read: true,
				write: false,
			},
			native: {},
		});

		await this.setObjectNotExistsAsync(`devices.${adptName}.oneDeviceLowBat`, {
			type: 'state',
			common: {
				name: {
					en: 'Is one device with low battery',
					de: 'Ist ein Gerät mit niedrigem Akku',
					ru: 'Один прибор с низкой батареей',
					pt: 'É um dispositivo com bateria baixa',
					nl: 'Is een apparaat met lage batterijen',
					fr: 'Est un appareil avec batterie basse',
					it: 'È un dispositivo con batteria bassa',
					es: 'Es un dispositivo con batería baja',
					pl: 'Jest to jeden urządzenie z niską baterią',
					// @ts-ignore
					uk: 'Є одним пристроєм з низьких акумуляторів',
					'zh-cn': '低电池的装置',
				},
				type: 'boolean',
				role: 'state',
				read: true,
				write: false,
				def: false,
			},
			native: {},
		});

		await this.setObjectNotExistsAsync(`devices.${adptName}.batteryCount`, {
			type: 'state',
			common: {
				name: {
					en: 'Number of devices with battery',
					de: 'Anzahl der Geräte mit Batterie',
					ru: 'Количество устройств c батареей',
					pt: 'Número de dispositivos com bateria',
					nl: 'Nummer van apparaten met batterij',
					fr: 'Nombre de dispositifs avec batterie',
					it: 'Numero di dispositivi con batteria',
					es: 'Número de dispositivos con batería',
					pl: 'Liczba urządzeń z baterią',
					'zh-cn': '电池的装置数量',
				},
				type: 'number',
				role: 'value',
				read: true,
				write: false,
			},
			native: {},
		});

		await this.setObjectNotExistsAsync(`devices.${adptName}.upgradableCount`, {
			type: 'state',
			common: {
				name: {
					en: 'Number of devices with available updates ',
					de: 'Anzahl der Geräte mit verfügbaren Updates',
					ru: 'Количество устройств с доступными обновлениями',
					pt: 'Número de dispositivos com atualizações disponíveis',
					nl: 'Nummer van apparatuur met beschikbare updates',
					fr: 'Nombre de dispositifs avec mises à jour disponibles',
					it: 'Numero di dispositivi con aggiornamenti disponibili',
					es: 'Número de dispositivos con actualizaciones disponibles',
					pl: 'Liczba urządzeń z dostępną aktualizacją',
					// @ts-ignore
					uk: 'Кількість пристроїв з доступними оновленнями',
					'zh-cn': '现有更新的装置数目',
				},
				type: 'number',
				role: 'value',
				read: true,
				write: false,
			},
			native: {},
		});

		await this.setObjectNotExistsAsync(`devices.${adptName}.upgradableList`, {
			type: 'state',
			common: {
				name: {
					en: 'JSON List of devices with available updates ',
					de: 'JSON Liste der Geräte mit verfügbaren Updates',
					ru: 'ДЖСОН Список устройств с доступными обновлениями',
					pt: 'J. Lista de dispositivos com atualizações disponíveis',
					nl: 'JSON List van apparatuur met beschikbare updates',
					fr: 'JSON Liste des appareils avec mises à jour disponibles',
					it: 'JSON Elenco dei dispositivi con aggiornamenti disponibili',
					es: 'JSON Lista de dispositivos con actualizaciones disponibles',
					pl: 'JSON Lista urządzeń korzystających z aktualizacji',
					// @ts-ignore
					uk: 'Сонце Перелік пристроїв з доступними оновленнями',
					'zh-cn': '附 件 现有最新设备清单',
				},
				type: 'array',
				role: 'json',
				read: true,
				write: false,
			},
			native: {},
		});

		await this.setObjectNotExistsAsync(`devices.${adptName}.oneDeviceUpdatable`, {
			type: 'state',
			common: {
				name: {
					en: 'Is one device updatable',
					de: 'Ist ein Gerät aufnehmbar',
					ru: 'Одно устройство обновляется',
					pt: 'É um dispositivo updatable',
					nl: 'Is een apparaat updat',
					fr: "Est-ce qu'un appareil est indéfectible",
					it: 'È un dispositivo updatable',
					es: 'Es un dispositivo actualizado',
					pl: 'Jest to jedno urządzenie updatable',
					// @ts-ignore
					uk: 'Є одним пристроєм',
					'zh-cn': '一台装置',
				},
				type: 'boolean',
				role: 'state',
				read: true,
				write: false,
				def: false,
			},
			native: {},
		});
	}

	/**
	 * delete datapoints for each adapter
	 * @param {object} adptName - Adaptername of devices
	 */
	async deleteDPsForEachAdapter(adptName) {
		await this.delObjectAsync(`devices.${adptName}`);
		await this.delObjectAsync(`devices.${adptName}.offlineCount`);
		await this.delObjectAsync(`devices.${adptName}.offlineList`);
		await this.delObjectAsync(`devices.${adptName}.oneDeviceOffline`);
		await this.delObjectAsync(`devices.${adptName}.listAllRawJSON`);
		await this.delObjectAsync(`devices.${adptName}.listAll`);
		await this.delObjectAsync(`devices.${adptName}.linkQualityList`);
		await this.delObjectAsync(`devices.${adptName}.countAll`);
		await this.delObjectAsync(`devices.${adptName}.batteryList`);
		await this.delObjectAsync(`devices.${adptName}.lowBatteryList`);
		await this.delObjectAsync(`devices.${adptName}.lowBatteryCount`);
		await this.delObjectAsync(`devices.${adptName}.oneDeviceLowBat`);
		await this.delObjectAsync(`devices.${adptName}.batteryCount`);
		await this.delObjectAsync(`devices.${adptName}.upgradableCount`);
		await this.delObjectAsync(`devices.${adptName}.upgradableList`);
		await this.delObjectAsync(`devices.${adptName}.oneDeviceUpdatable`);
	}

	/**
	 * create HTML list datapoints
	 * @param {object} [adptName] - Adaptername of devices
	 **/
	async createHtmlListDatapoints(adptName) {
		let dpSubFolder;
		//write the datapoints in subfolders with the adaptername otherwise write the dP's in the root folder
		if (adptName) {
			dpSubFolder = `${adptName}.`;
		} else {
			dpSubFolder = '';
		}

		await this.setObjectNotExistsAsync(`devices.${dpSubFolder}offlineListHTML`, {
			type: 'state',
			common: {
				name: {
					en: 'HTML List of offline devices',
					de: 'HTML Liste der Offline-Geräte',
					ru: 'HTML Список оффлайн устройств',
					pt: 'HTML Lista de dispositivos off-line',
					nl: 'HTML List van offline apparatuur',
					fr: 'HTML Liste des dispositifs hors ligne',
					it: 'HTML Elenco dei dispositivi offline',
					es: 'HTML Lista de dispositivos sin conexión',
					pl: 'HTML Lista urządzeń offline',
					'zh-cn': 'HTML 线装置清单',
				},
				type: 'string',
				role: 'html',
				read: true,
				write: false,
			},
			native: {},
		});

		await this.setObjectNotExistsAsync(`devices.${dpSubFolder}linkQualityListHTML`, {
			type: 'state',
			common: {
				name: {
					en: 'HTML List of devices with signal strength',
					de: 'HTML Liste der Geräte mit Signalstärke',
					ru: 'HTML Список устройств с силой сигнала',
					pt: 'HTML Lista de dispositivos com força de sinal',
					nl: 'HTML List van apparaten met signaalkracht',
					fr: 'HTML Liste des dispositifs avec force de signal',
					it: 'HTML Elenco dei dispositivi con forza del segnale',
					es: 'HTML Lista de dispositivos con fuerza de señal',
					pl: 'HTML Lista urządzeń z siłą sygnałową',
					'zh-cn': 'HTML 具有信号实力的装置清单',
				},
				type: 'string',
				role: 'value',
				read: true,
				write: false,
			},
			native: {},
		});

		await this.setObjectNotExistsAsync(`devices.${dpSubFolder}batteryListHTML`, {
			type: 'state',
			common: {
				name: {
					en: 'HTML List of devices with battery state',
					de: 'HTML Liste der Geräte mit Batteriezustand',
					ru: 'HTML Список устройств с состоянием батареи',
					pt: 'HTML Lista de dispositivos com estado da bateria',
					nl: 'HTML List van apparaten met batterij staat',
					fr: 'HTML Liste des appareils avec état de batterie',
					it: 'HTML Elenco dei dispositivi con stato della batteria',
					es: 'HTML Lista de dispositivos con estado de batería',
					pl: 'HTML Lista urządzeń z baterią stanową',
					'zh-cn': 'HTML 电池国装置清单',
				},
				type: 'string',
				role: 'html',
				read: true,
				write: false,
			},
			native: {},
		});

		await this.setObjectNotExistsAsync(`devices.${dpSubFolder}lowBatteryListHTML`, {
			type: 'state',
			common: {
				name: {
					en: 'HTML List of devices with low battery state',
					de: 'HTML Liste der Geräte mit niedrigem Batteriezustand',
					ru: 'HTML Список устройств с низким состоянием батареи',
					pt: 'HTML Lista de dispositivos com baixo estado da bateria',
					nl: 'HTML List van apparaten met lage batterij staat',
					fr: 'HTML Liste des appareils à faible état de batterie',
					it: 'HTML Elenco di dispositivi con stato di batteria basso',
					es: 'HTML Lista de dispositivos con estado de batería bajo',
					pl: 'HTML Lista urządzeń o niskim stanie baterii',
					'zh-cn': 'HTML 低电池国家装置清单',
				},
				type: 'string',
				role: 'html',
				read: true,
				write: false,
			},
			native: {},
		});
	}

	/**
	 * delete html datapoints
	 * @param {object} [adptName] - Adaptername of devices
	 **/
	async deleteHtmlListDatapoints(adptName) {
		// delete the datapoints in subfolders with the adaptername otherwise delete the dP's in the root folder
		let dpSubFolder;
		if (adptName) {
			dpSubFolder = `${adptName}.`;
		} else {
			dpSubFolder = '';
		}

		await this.delObjectAsync(`devices.${dpSubFolder}offlineListHTML`);
		await this.delObjectAsync(`devices.${dpSubFolder}linkQualityListHTML`);
		await this.delObjectAsync(`devices.${dpSubFolder}batteryListHTML`);
		await this.delObjectAsync(`devices.${dpSubFolder}lowBatteryListHTML`);
	}

	/**
	 * create HTML list datapoints for instances
	 **/
	async createHtmlListDatapointsInstances() {
		await this.setObjectNotExistsAsync(`adapterAndInstances.HTML_Lists`, {
			type: 'channel',
			common: {
				name: {
					en: 'HTML lists for adapter and instances',
					de: 'HTML-Listen für Adapter und Instanzen',
					ru: 'HTML-списки для адаптеров и инстанций',
					pt: 'Listas HTML para adaptador e instâncias',
					nl: 'HTML lijsten voor adapter en instituut',
					fr: "Listes HTML pour l'adaptateur et les instances",
					it: 'Elenchi HTML per adattatore e istanze',
					es: 'Listas HTML para adaptador y casos',
					pl: 'Listy HTML dla adaptera i instancji',
					// @ts-ignore
					uk: 'Списки HTML для адаптерів та екземплярів',
					'zh-cn': 'HTML名单',
				},
			},
			native: {},
		});
		await this.setObjectNotExistsAsync(`adapterAndInstances.HTML_Lists.listAllInstancesHTML`, {
			type: 'state',
			common: {
				name: {
					en: 'HTML List of all instances',
					de: 'HTML Liste aller Instanzen',
					ru: 'HTML Список всех инстанций',
					pt: 'HTML Lista de todas as instâncias',
					nl: 'HTM List van alle instanties',
					fr: 'HTML Liste de tous les cas',
					it: 'HTML Elenco di tutte le istanze',
					es: 'HTML Lista de todos los casos',
					pl: 'HTML Lista wszystkich instancji',
					// @ts-ignore
					uk: 'Українська Список всіх екземплярів',
					'zh-cn': 'HTML 所有事例一览表',
				},
				type: 'string',
				role: 'html',
				read: true,
				write: false,
			},
			native: {},
		});

		await this.setObjectNotExistsAsync(`adapterAndInstances.HTML_Lists.listAllActiveInstancesHTML`, {
			type: 'state',
			common: {
				name: {
					en: 'HTML List of all active instances',
					de: 'HTML Liste aller aktiven Instanzen',
					ru: 'HTML Список всех активных инстанций',
					pt: 'HTML Lista de todas as instâncias ativas',
					nl: 'HTM List van alle actieve instanties',
					fr: 'HTML Liste de tous les cas actifs',
					it: 'HTML Elenco di tutte le istanze attive',
					es: 'HTML Lista de todos los casos activos',
					pl: 'HTML Lista wszystkich aktywnych instancji',
					// @ts-ignore
					uk: 'Українська Список всіх активних екземплярів',
					'zh-cn': 'HTML 所有积极事件清单',
				},
				type: 'string',
				role: 'value',
				read: true,
				write: false,
			},
			native: {},
		});

		await this.setObjectNotExistsAsync(`adapterAndInstances.HTML_Lists.listDeactivatedInstancesHTML`, {
			type: 'state',
			common: {
				name: {
					en: 'HTML List of all deactivated instances',
					de: 'HTML Liste aller deaktivierten Instanzen',
					ru: 'HTML Список всех деактивированных инстанций',
					pt: 'HTML Lista de todas as instâncias desativadas',
					nl: 'HTM List van alle gedeactiveerde instanties',
					fr: 'HTML Liste de tous les cas désactivés',
					it: 'HTML Elenco di tutte le istanze disattivate',
					es: 'HTML Lista de todos los casos desactivados',
					pl: 'HTML Lista wszystkich przypadków deaktywowanych',
					// @ts-ignore
					uk: 'Українська Список всіх деактивованих екземплярів',
					'zh-cn': 'HTML 所有违犯事件清单',
				},
				type: 'string',
				role: 'html',
				read: true,
				write: false,
			},
			native: {},
		});

		await this.setObjectNotExistsAsync(`adapterAndInstances.HTML_Lists.listInstancesErrorHTML`, {
			type: 'state',
			common: {
				name: {
					en: 'HTML List of instances with error',
					de: 'HTML Liste der Fälle mit Fehler',
					ru: 'HTML Список инстанций с ошибкой',
					pt: 'HTML Lista de casos com erro',
					nl: 'HTM List van instoringen met fouten',
					fr: 'HTML Liste des instances avec erreur',
					it: 'HTML Elenco delle istanze con errore',
					es: 'HTML Lista de casos con error',
					pl: 'HTML Lista przykładów z błądem',
					// @ts-ignore
					uk: 'Українська Список екземплярів з помилкою',
					'zh-cn': 'HTML 出现错误的情况清单',
				},
				type: 'string',
				role: 'html',
				read: true,
				write: false,
			},
			native: {},
		});
		await this.setObjectNotExistsAsync(`adapterAndInstances.HTML_Lists.listAdapterUpdatesHTML`, {
			type: 'state',
			common: {
				name: {
					en: 'HTML list of adapters with available updates',
					de: 'HTML-Liste der Adapter mit verfügbaren Updates',
					ru: 'HTML список адаптеров с доступными обновлениями',
					pt: 'Lista HTML de adaptadores com atualizações disponíveis',
					nl: 'HTML lijst met beschikbare updates',
					fr: 'Liste HTML des adaptateurs avec mises à jour disponibles',
					it: 'Elenco HTML degli adattatori con aggiornamenti disponibili',
					es: 'Lista HTML de adaptadores con actualizaciones disponibles',
					pl: 'Lista adapterów HTML z dostępnymi aktualizacjami',
					// @ts-ignore
					uk: 'HTML список адаптерів з доступними оновленнями',
					'zh-cn': 'HTML 可供更新的适应者名单',
				},
				type: 'string',
				role: 'html',
				read: true,
				write: false,
			},
			native: {},
		});
	}

	/**
	 * delete html datapoints for instances
	 **/
	async deleteHtmlListDatapointsInstances() {
		await this.delObjectAsync(`adapterAndInstances.HTML_Lists.listAllInstancesHTML`);
		await this.delObjectAsync(`adapterAndInstances.HTML_Lists.listAllActiveInstancesHTML`);
		await this.delObjectAsync(`adapterAndInstances.HTML_Lists.listDeactivatedInstancesHTML`);
		await this.delObjectAsync(`adapterAndInstances.HTML_Lists.listInstancesErrorHTML`);
		await this.delObjectAsync(`adapterAndInstances.HTML_Lists.listAdapterUpdatesHTML`);
		await this.delObjectAsync(`adapterAndInstances.HTML_Lists`);
	}

	/*=============================================
	=            	help functions   	          =
	=============================================*/

	/**
	 * @param {string} id - id which should be capitalize
	 */
	capitalize(id) {
		//make the first letter uppercase
		return id && id[0].toUpperCase() + id.slice(1);
	}

	/**
	 * @param {number} dpValue - get Time of this datapoint
	 */
	getTimestamp(dpValue) {
		const time = new Date();
		return (dpValue = Math.round((time.getTime() - dpValue) / 1000 / 60));
	}

	/**
	 * @param {string} dp - get Time of this datapoint
	 * @param {number} ms - milliseconds
	 */
	async getTimestampConnectionDP(dp, ms) {
		const time = new Date();
		const dpValue = await this.getForeignStateAsync(dp);
		if (dpValue) {
			if (!dpValue.val) return false;

			const dpLastStateChange = Math.round(time.getTime() - dpValue.lc); // calculate in ms
			if (dpLastStateChange >= ms) {
				return true;
			} else {
				return false;
			}
		}
	}

	/**
	 * @param {object} obj - State of datapoint
	 */
	async getInitValue(obj) {
		//state can be null or undefinded
		const foreignState = await this.getForeignStateAsync(obj);
		if (foreignState) return foreignState.val;
	}

	/**
	 * @param {object} obj - State of own datapoint
	 */
	async getOwnInitValue(obj) {
		//state can be null or undefinded for own states
		const stateVal = await this.getStateAsync(obj);
		if (stateVal) return stateVal.val;
	}

	/**
	 * @param {object} data - object
	 */
	parseData(data) {
		if (!data) return {};
		if (typeof data === 'object') return data;
		if (typeof data === 'string') return JSON.parse(data);
		return {};
	}

	/**
	 * Get previous run of cron job schedule
	 * Requires cron-parser!
	 * Inspired by https://stackoverflow.com/questions/68134104/
	 * @param {string} lastCronRun
	 */
	getPreviousCronRun(lastCronRun) {
		try {
			const interval = cronParser.parseExpression(lastCronRun);
			const previous = interval.prev();
			return Math.floor(Date.now() - previous.getTime()); // in ms
		} catch (error) {
			this.log.error(`[getPreviousCronRun] - ${error}`);
		}
	}

	/**
	 * @param {() => void} callback
	 */
	onUnload(callback) {
		try {
			this.log.debug('clearing timeouts');

			isUnloaded = true;

			if (this.refreshDataTimeout) {
				this.clearTimeout(this.refreshDataTimeout);
				this.refreshDataTimeout = null;
			}

			this.log.info('cleaned everything up...');

			callback();
		} catch (e) {
			callback();
		}
	}
}

// @ts-ignore parent is a valid property on module
if (require.main !== module) {
	// Export the constructor in compact mode
	/**
	 * @param {Partial<utils.AdapterOptions>} [options={}]
	 */
	module.exports = (options) => new DeviceWatcher(options);
} else {
	// otherwise start the instance directly
	new DeviceWatcher();
}
