from behave import given, when, then

@given(u'a volume is created')
def step_impl(context):
    # start the k8s job with the fio configuration
    raise NotImplementedError(u'STEP: Given a volume is created')


@when(u'a node with no lvols connected is suspended')
def step_impl(context):
    raise NotImplementedError(u'STEP: When a node with no lvols connected is suspended')


@then(u'the node is in offline status')
def step_impl(context):
    raise NotImplementedError(u'STEP: Then the node is in offline status')


@then(u'all the nodes are online')
def step_impl(context):
    raise NotImplementedError(u'STEP: Then all the nodes are online')


@then(u'the cluster is in degraded state')
def step_impl(context):
    raise NotImplementedError(u'STEP: Then the cluster is in degraded state')


@then(u'the devices of the node are in unavailable state')
def step_impl(context):
    raise NotImplementedError(u'STEP: Then the devices of the node are in unavailable state')


@then(u'the event log contains the records indicating the object status changes')
def step_impl(context):
    raise NotImplementedError(u'STEP: Then the event log contains the records indicating the object status changes')


@then(u'check if fio is still running')
def step_impl(context):
    raise NotImplementedError(u'STEP: Then check if fio is still running')
