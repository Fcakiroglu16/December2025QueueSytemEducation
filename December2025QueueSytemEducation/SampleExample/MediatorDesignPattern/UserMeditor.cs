using System;
using System.Collections.Generic;
using System.Text;

namespace December2025QueueSystemEducation.SampleExample.MediatorDesignPattern
{
    public class UserMeditor
    {

        private List<IUserAddRequestHandler> m_Handlers;

        public UserMeditor()
        {
            m_Handlers = new List<IUserAddRequestHandler>();
        }

        public void RegisterHandler(IUserAddRequestHandler handler)
        {
            m_Handlers.Add(handler);
        }

        public void UnregisterHandler(IUserAddRequestHandler handler)
        {
            m_Handlers.Remove(handler);
        }

        public void Handle(IUserAddRequest request)
        {
            foreach (var handler in m_Handlers)
            {
                handler.Handle(request.user);
            }
        }
    }
}
