import os
import sys
import json
import time
import logging
import threading
import tornado.httpclient
from tornado.ioloop import IOLoop
from proton.reactor import Container
from proton.handlers import MessagingHandler
from tornado.web import Application, RequestHandler

#############################################################################################
################################ Logging #################################
#############################################################################################

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

def logger_setup(name, level=logging.DEBUG):
    """Setup different loggers here"""

    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(sh)
    logger.propagate = False

    return logger

def logger_file_setup(name, file_name, level=os.environ['LOG_LEVEL']):
    """Setup different file-loggers here"""

    file_handler = logging.FileHandler(file_name)
    file_handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(file_handler)
    
    return logger

general_log = logger_setup(os.environ['LOGGER_NAME'])
time_log = logger_setup(' Timing Local Manager 1 ')

#general_log = logger_file_setup(os.environ['LOGGER_NAME'],os.environ['LOG_PATH_GENERAL'])
#time_log = logger_file_setup(' Timing Local Manager 1 ','/logs/lm1time.log')

#############################################################################################
################################ Logging #################################
#############################################################################################


################################################################

class TraceThread(threading.Thread):
  """ Simple thread class to kill threads using traces"""

  def __init__(self, *args, **keywords):
    threading.Thread.__init__(self, *args, **keywords)
    self.killed = False
  
  def start(self):
    self.__run_backup = self.run
    self.run = self.__run     
    threading.Thread.start(self)
  
  def __run(self):
    sys.settrace(self.globaltrace)
    self.__run_backup()
    self.run = self.__run_backup

  def globaltrace(self, frame, why, arg):
    if why == 'call':
      return self.localtrace
    else:
      return None
  
  def localtrace(self, frame, why, arg):
    if self.killed:
      if why == 'line':
        raise SystemExit()
    return self.localtrace
  
  def kill(self):
    self.killed = True

#################################################################


#######################################################################################
#########################TENTATIVE CONFIG DICTS########################################
#######################################################################################

sm_dict={}
qtcode_dict={}

def get_config(json_config):
    amqp_ep = json_config["ref_amqpep"]
    for lm in json_config["local_config"]:
        for qt in lm["qtcode_list"]:
            qtcode_dict.update({qt:lm["message"].get("id")})
            sm_dict.update({lm["message"].get("id"):lm["message"].get("endpoints")})
    return sm_dict,qtcode_dict,amqp_ep



#######################################################################################
#########################TENTATIVE CONFIG DICTS########################################
#######################################################################################



class Subscriber(MessagingHandler):
    def __init__(self, server, receive_topic_names):
        super(Subscriber, self).__init__()
        self.server = server
        self.receive_topic_names = receive_topic_names
        self.receivers = dict()
        self.car_details = dict()
        self.car_ref_time = 0
        self.curr_location = ""
        self.user = os.environ['MSG_BROKER_USER']
        self.password = os.environ['MSG_BROKER_PASSWORD']
        self.receive_msg_time = 0 
        self.time_and_position = dict()
        self.sm_time_type = dict()
        self.connection = None
        self.timeout_limit_max = 8
        self.timeout_limit_min = 1
        self.timeout_limit = 1



    def on_start(self, event):
        conn = event.container.connect(self.server, user=self.user, password=self.password)
        for topic in self.receive_topic_names:
            self.receivers.update({topic:event.container.create_receiver(conn, 'topic://%s' % topic)})
        #"quadTree='A1' OR quadTree='A2' OR quadTree='A3' OR quadTree='B1' OR quadTree='B2' OR quadTree='B3'"
        self.connection = conn
    
    def on_disconnected(self, event):
        """ Triggers the disconected event when the link is lost"""

        general_log.error("The connection to broker is lost. Trying to reestablish the connection")
        self.connection.close()

        if self.timeout_limit < self.timeout_limit_max:
            time.sleep(self.timeout_limit)
            conn = event.container.connect(self.server, user=self.user, password=self.password)
            
            print("waited for "+str(self.timeout_limit)+" seconds\n")
            for topic in self.receive_topic_names:
                self.receivers.update({topic:event.container.create_receiver(conn, 'topic://%s' % topic)})
            self.connection = conn
            self.timeout_limit*=2
            print(str(self.get_connection_state())+" connection state\n")
            
        else:
            time.sleep(self.timeout_limit)
            conn = event.container.connect(self.server, user=self.user, password=self.password)

            print("waited for "+str(self.timeout_limit)+" seconds\n")
            for topic in self.receive_topic_names:
                self.receivers.update({topic:event.container.create_receiver(conn, 'topic://%s' % topic)})
            self.connection = conn
        
        return super().on_disconnected(event)


    def get_connection_state(self):
        try:
            state = self.connection.state
            if state == 18:
                self.timeout_limit = self.timeout_limit_min
        except Exception:
            return 0
        return state

    def on_message(self, event):
        self.receive_msg_time = time.time()
        self.car_ref_time = float(event.message.properties["timestamp"])
        data = json.loads(event.message.body)
        car_id = data["Car_ID"]
        self.curr_location = event.message.properties["quadTree"]

        general_log.info("Car "+str(car_id)+" is in "+self.curr_location)
        
        if self.curr_location in qtcode_dict.keys():
            if car_id not in self.car_details:
                self.sm_time_type.update({"sm_type":qtcode_dict[self.curr_location], "sm_sent_timestamp":time.time()})
                self.time_and_position.update({"location":self.curr_location,"receive_timestamp":self.receive_msg_time, "support_message":self.sm_time_type})
                self.car_details.update({car_id:self.time_and_position})

                bjson = json.dumps({'messages':[{'Car_ID':car_id,\
                                     'message':sm_dict[qtcode_dict[self.curr_location]], 'timestamp_lm':time.time(), 'ref_timestamp_fc':self.car_ref_time}]})
                change_ep_of_car(bjson)
                send_change_ep_msg = time.time()
                time_log.debug(str((send_change_ep_msg-self.receive_msg_time)*1000)+" ms from reception of msg sent from car to change of endpoint\n")

            else:
                retry_threshold = time.time()-self.car_details[car_id]["support_message"]["sm_sent_timestamp"]
                if (retry_threshold > 5 and qtcode_dict[self.curr_location] == self.sm_time_type["sm_type"])  or (qtcode_dict[self.curr_location] != self.sm_time_type["sm_type"]):
                    self.sm_time_type.update({"sm_type":qtcode_dict[self.curr_location], "sm_sent_timestamp":time.time()})
                    self.time_and_position.update({"location":self.curr_location,"receive_timestamp":self.receive_msg_time, "support_message":self.sm_time_type})
                    self.car_details.update({car_id:self.time_and_position})
                    
                    bjson = json.dumps({'messages':[{'Car_ID':car_id,\
                                     'message':sm_dict[qtcode_dict[self.curr_location]], 'timestamp_lm':time.time(), 'ref_timestamp_fc':self.car_ref_time}]})
                    change_ep_of_car(bjson)
                    send_change_ep_msg = time.time()
                    time_log.debug(str((send_change_ep_msg-self.receive_msg_time)*1000)+" ms from reception of msg sent from car to change of endpoint\n")

        else:
            bjson = json.dumps({'messages':[{'Car_ID':car_id,\
                                     'message':sm_dict[qtcode_dict["OUT"]], 'timestamp_lm':time.time(), 'ref_timestamp_fc':self.car_ref_time}]})
            change_ep_of_car(bjson)
            send_change_ep_msg = time.time()
            time_log.debug(str((send_change_ep_msg-self.receive_msg_time)*1000)+" ms from reception of msg sent from car to change of endpoint\n")


        

AMQP_Addr=""


def kill_old_threads():
    """ Kill older threads in case of config update"""

    for i in threading.enumerate():
        if i is not threading.main_thread() and isinstance(i, TraceThread):
            i.kill()
        else:
            pass


class MMtoLMConfig(RequestHandler):
    """ API SERVER for handling calls from the main manager regarding LM configuration"""
    
    def post(self, id):
        """Handles the behaviour of POST calls"""
        json_form = json.loads(self.request.body)
        lm_sm_config,lm_qt_config,amqp_ep = get_config(json_form)
        if threading.active_count() > 1:
            kill_old_threads()
        topics = ["FROM_CARS"]
        client_sub = Subscriber(amqp_ep, topics)#163.162.42.24:5672  
        container = Container(client_sub)
        qpid_thread_sub = TraceThread(target=container.run)
        qpid_thread_sub.start()

    def put(self, id):
        """Handles the behaviour of PUT calls"""
        self.write("Not supposed to PUT!")
    
    def get(self, id):
        """Handles the behaviour of GET calls"""
        self.write('NOT SUPPOSED TO GET!')


    def delete(self, id):
        """Handles the behaviour of DELETE calls"""
        self.write("Not supposed to DELETE!")
    


class MMtoLMCar(RequestHandler):
    """ API SERVER for handling calls from the main manager """

    def post(self, id):
        """Handles the behaviour of POST calls"""
        self.write(json.loads(self.request.body))
        #AMQP_Addr = json.loads(self.request.body)["local_manager_id"]
        json_form = json.loads(self.request.body)

    def put(self, id):
        """Handles the behaviour of PUT calls"""
        global items
        new_items = [item for item in items if item['id'] is not int(id)]
        items = new_items
        self.write({'message': 'Item with id %s was updated' % id})


    def delete(self, id):
        """Handles the behaviour of DELETE calls"""
        global items
        new_items = [item for item in items if item['id'] is not int(id)]
        items = new_items
        self.write({'message': 'Item with id %s was deleted' % id})



def change_ep_of_car(bjson):
    """ To change the endpoint of the car in case of chaging zones """
    #start_change_ep = time.time()
    try:
        response = http_client.fetch(os.environ['RESPONSE_ROUTER_POST_ADDRESS'],method='POST',body=bjson)        
    except Exception as e:
        general_log.error("Error: %s" % e)
        general_log.error("Couldn't POST to Response Router")
    else:
        general_log.debug(response.body)
        #general_log.debug(str((time.time()-start_change_ep)*1000)+" ms to POST")

def make_app():
  urls = [
    (r"/api/item/from_main_mgr_config_api/([^/]+)?", MMtoLMConfig),
    (r"/api/item/from_main_mgr_car_reg_api/([^/]+)?", MMtoLMCar)
  ]
  return Application(urls, debug=True)


  
if __name__ == '__main__':

  app = make_app()
  app.listen(os.environ['API_PORT'])
  print("Started Local Manager 1 REST Server",flush=True)
  http_client = tornado.httpclient.HTTPClient()
  IOLoop.instance().start()
