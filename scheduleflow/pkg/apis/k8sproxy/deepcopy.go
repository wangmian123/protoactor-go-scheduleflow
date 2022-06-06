package k8sproxy

func (gvr *GroupVersionResource) DeepCopy() *GroupVersionResource {
	return &GroupVersionResource{
		Group:    gvr.Group,
		Version:  gvr.Version,
		Resource: gvr.Resource,
	}
}

func (c *CreateEvent) DeepCopy() *CreateEvent {
	rawCopy := make([]byte, len(c.GetRawResource()))
	copy(rawCopy, c.RawResource)
	return &CreateEvent{
		Timestamp:   c.Timestamp.DeepCopy(),
		GVR:         c.GVR.DeepCopy(),
		RawResource: rawCopy,
	}
}

func (c *DeleteEvent) DeepCopy() *DeleteEvent {
	rawCopy := make([]byte, len(c.GetRawResource()))
	copy(rawCopy, c.RawResource)
	return &DeleteEvent{
		Timestamp:   c.Timestamp.DeepCopy(),
		GVR:         c.GVR.DeepCopy(),
		RawResource: rawCopy,
	}
}

func (c *UpdateEvent) DeepCopy() *UpdateEvent {
	rawCopyOld := make([]byte, len(c.GetOldResource()))
	rawCopyNew := make([]byte, len(c.GetNewResource()))
	copy(rawCopyOld, c.OldResource)
	copy(rawCopyNew, c.NewResource)
	return &UpdateEvent{
		Timestamp:   c.Timestamp.DeepCopy(),
		GVR:         c.GVR.DeepCopy(),
		OldResource: rawCopyOld,
		NewResource: rawCopyNew,
	}
}
